// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadindexrec

import (
	"context"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// FindWorkloadRecs finds index recommendations for the whole workload after the
// timestamp ts within the space budget represented by budgetBytes.
func FindWorkloadRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ, hasStatistics bool,
) ([]string, []json.JSON, error) {
	cis, dis, fingerprints, executionCnts, err := collectIndexRecs(ctx, evalCtx, ts, hasStatistics)
	if err != nil {
		return nil, nil, err
	}

	trieMap := buildTrieForIndexRecs(cis, fingerprints, executionCnts)
	newCis, statistics, err := extractIndexCovering(trieMap, hasStatistics)
	if err != nil {
		return nil, nil, err
	}

	var res = make([]string, len(newCis))

	for i, ci := range newCis {
		res[i] = ci.String() + ";"
	}

	// Since we collect all the indexes represented by the leaf nodes, all the
	// indexes with "DROP INDEX" has been covered, so we can directly drop all of
	// them without duplicates.
	var disMap = make(map[tree.TableIndexName]bool)
	for _, di := range dis {
		for _, index := range di.IndexList {
			disMap[*index] = true
		}
	}

	for index := range disMap {
		dropCmd := tree.DropIndex{
			IndexList: []*tree.TableIndexName{&index},
		}
		res = append(res, dropCmd.String()+";")
	}

	return res, statistics, nil
}

// collectIndexRecs collects all the index recommendations stored in the
// system.statement_statistics with the time later than ts.
func collectIndexRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ, hasStatistics bool,
) ([]tree.CreateIndex, []tree.DropIndex, []string, []int, error) {
	query := `SELECT index_recommendations{$1} FROM system.statement_statistics
						 WHERE (statistics -> 'statistics' ->> 'lastExecAt')::TIMESTAMPTZ > $2
						 AND array_length(index_recommendations, 1) > 0;`
	statisticsCol := ``
	if hasStatistics {
		statisticsCol = `, fingerprint_id, execution_count`
	}

	results, err := evalCtx.Planner.QueryIteratorEx(ctx, "get-candidates-for-workload-indexrecs",
		sessiondata.NoSessionDataOverride, query, statisticsCol, ts.Time)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var p parser.Parser
	var cis []tree.CreateIndex
	var dis []tree.DropIndex
	var fingerprints []string
	var executionCnts []int
	var ok bool

	// The index recommendation starts with "creation", "replacement" or
	// "alteration".
	var r = regexp.MustCompile(`\s*(creation|replacement|alteration)\s*:\s*(.*)`)

	for ok, err = results.Next(ctx); ; ok, err = results.Next(ctx) {
		if err != nil {
			err = errors.CombineErrors(err, results.Close())
			results = nil
			return cis, dis, fingerprints, executionCnts, nil
		}

		if !ok {
			break
		}

		indexes := tree.MustBeDArray(results.Cur()[0])
		for _, index := range indexes.Array {
			indexStr, ok := index.(*tree.DString)
			if !ok {
				err = errors.CombineErrors(errors.Newf("%s is not a string!", index.String()), results.Close())
				results = nil
				return cis, dis, fingerprints, executionCnts, nil
			}

			indexStrArr := r.FindStringSubmatch(string(*indexStr))
			if indexStrArr == nil {
				err = errors.CombineErrors(errors.Newf("%s is not a valid index recommendation!", string(*indexStr)), results.Close())
				results = nil
				return cis, dis, fingerprints, executionCnts, nil
			}

			// Since Alter index recommendation only makes invisible indexes visible,
			// so we skip it for now.
			if indexStrArr[1] == "alteration" {
				continue
			}

			stmts, err := p.Parse(indexStrArr[2])
			if err != nil {
				err = errors.CombineErrors(errors.Newf("%s is not a valid index operation!", indexStrArr[2]), results.Close())
				results = nil
				return cis, dis, fingerprints, executionCnts, nil
			}

			for _, stmt := range stmts {
				switch stmt := stmt.AST.(type) {
				case *tree.CreateIndex:
					// Ignore all the inverted, partial and sharded indexes right now.
					if !stmt.Inverted && stmt.Predicate == nil && stmt.Sharded == nil {
						cis = append(cis, *stmt)
					}
				case *tree.DropIndex:
					dis = append(dis, *stmt)
				}
			}
		}

		if hasStatistics {
			fingerprint := tree.MustBeDBytes(results.Cur()[1])
			fingerprints = append(fingerprints, string(fingerprint))

			executionCnt := tree.MustBeDInt(results.Cur()[2])
			executionCnts = append(executionCnts, int(executionCnt))
		}
	}

	return cis, dis, fingerprints, executionCnts, nil
}

// buildTrieForIndexRecs builds the relation among all the indexRecs by a trie tree.
func buildTrieForIndexRecs(
	cis []tree.CreateIndex, fingerprints []string, executionCnts []int,
) map[tree.TableName]*indexTrie {
	trieMap := make(map[tree.TableName]*indexTrie)
	var fingerprint string
	var executionCnt int
	for i, ci := range cis {
		if _, ok := trieMap[ci.Table]; !ok {
			trieMap[ci.Table] = NewTrie()
		}

		if i < len(fingerprints) {
			fingerprint = fingerprints[i]
		} else {
			fingerprint = ""
		}

		if i < len(executionCnts) {
			executionCnt = executionCnts[i]
		} else {
			executionCnt = 0
		}

		trieMap[ci.Table].Insert(ci.Columns, ci.Storing, fingerprint, executionCnt)
	}
	return trieMap
}

// extractIndexCovering pushes down the storing part of the internal nodes: find
// whether it is covered by some leaf nodes. If yes, discard it; Otherwise,
// assign it to the shallowest leaf node. Then extractIndexCovering collects all
// the indexes represented by the leaf node.
func extractIndexCovering(
	tm map[tree.TableName]*indexTrie, hasStatistics bool,
) ([]tree.CreateIndex, []json.JSON, error) {
	for _, t := range tm {
		t.RemoveStorings()
	}
	for _, t := range tm {
		t.AssignStoring()
	}
	var cis []tree.CreateIndex
	var statistics []json.JSON
	for table, trie := range tm {
		indexedColsArray, storingColsArray, fingerprintMaps, executionCnts := collectAllLeavesForTable(trie, hasStatistics)
		// The length of indexedCols and storingCols must be equal
		if len(indexedColsArray) != len(storingColsArray) {
			return nil, nil, errors.Newf("The length of indexedColsArray and storingColsArray after collecting leaves from table %s is not equal!", table)
		}

		if hasStatistics && len(fingerprintMaps) != len(indexedColsArray) {
			return nil, nil, errors.Newf("The length of indexedColsArray and fingerprintMaps after collecting leaves from table %s is not equal!", table)
		}
		if hasStatistics && len(executionCnts) != len(indexedColsArray) {
			return nil, nil, errors.Newf("The length of indexedColsArray and executionCnts after collecting leaves from table %s is not equal!", table)
		}

		for i, indexedCols := range indexedColsArray {
			cisIndexedCols := make([]tree.IndexElem, len(indexedCols))
			for j, col := range indexedCols {
				cisIndexedCols[j] = tree.IndexElem{
					Column:    col.column,
					Direction: col.direction,
				}
				// Recover the ASC to Default direction.
				if col.direction == tree.Ascending {
					cisIndexedCols[j].Direction = tree.DefaultDirection
				}
			}
			cis = append(cis, tree.CreateIndex{
				Table:   table,
				Columns: cisIndexedCols,
				Storing: storingColsArray[i],
			})

			if hasStatistics {

			}
		}
	}
	return cis, nil, nil
}
