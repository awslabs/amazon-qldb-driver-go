/*
 Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License").
 You may not use this file except in compliance with the License.
 A copy of the License is located at

 http://www.apache.org/licenses/LICENSE-2.0

 or in the "license" file accompanying this file. This file is distributed
 on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied. See the License for the specific language governing
 permissions and limitations under the License.
*/

package qldbdriver

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func cleanup(driver *QLDBDriver, testTableName string) {
	_, _ = driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
		return txn.Execute(fmt.Sprintf("DELETE FROM %s", testTableName))
	})
}

func TestStatementExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// setup
	testBase := createTestBase()
	testBase.deleteLedger(t)
	testBase.createLedger(t)

	qldbDriver, err := testBase.getDriver(ledger, 10, 4)
	require.NoError(t, err)

	_, err = qldbDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
		return txn.Execute(fmt.Sprintf("CREATE TABLE %s", testTableName))
	})
	require.NoError(t, err)

	executeWithParam := func(ctx context.Context, query string, txn Transaction, parameters ...interface{}) (interface{}, error) {
		result, err := txn.Execute(query, parameters...)
		if err != nil {
			return nil, err
		}
		count := 0
		for result.Next(txn) {
			count++
		}
		if result.Err() != nil {
			return nil, result.Err()
		}
		return count, nil
	}

	t.Run("Drop existing table", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())

		createTableName := "GoIntegrationTestCreateTable"
		createTableQuery := fmt.Sprintf("CREATE TABLE %s", createTableName)

		executeResult, err := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), createTableQuery, txn)
		})
		assert.NoError(t, err)
		actualResult := executeResult.(int)
		assert.Equal(t, 1, actualResult)

		tables, err := driver.GetTableNames(context.Background())
		assert.NoError(t, err)
		assert.True(t, contains(tables, createTableName))

		dropTableQuery := fmt.Sprintf("DROP TABLE %s", createTableName)
		dropResult, droperr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), dropTableQuery, txn)
		})
		assert.NoError(t, droperr)
		assert.Equal(t, 1, dropResult.(int))
	})

	t.Run("List tables", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		tables, err := driver.GetTableNames(context.Background())
		assert.NoError(t, err)
		assert.True(t, contains(tables, testTableName))
	})

	t.Run("Create table that exists", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		query := fmt.Sprintf("CREATE TABLE %s", testTableName)

		result, err := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return txn.Execute(query)
		})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.IsType(t, &qldbsession.BadRequestException{}, err)
		_, ok := err.(*qldbsession.BadRequestException)
		assert.True(t, ok)
	})

	t.Run("Create index", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		indexQuery := fmt.Sprintf("CREATE INDEX ON %s (%s)", testTableName, indexAttribute)
		indexResult, indexErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), indexQuery, txn)
		})
		assert.NoError(t, indexErr)
		assert.Equal(t, 1, indexResult.(int))

		// Wait for above index to be created before querying index
		time.Sleep(5 * time.Second)

		searchQuery := "SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE' "
		searchQuery += fmt.Sprintf("AND name = '%s'", testTableName)

		type exprName struct {
			Expr string `ion:"expr"`
		}

		searchRes, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery)
			if err != nil {
				return nil, err
			}
			if !result.Next(txn) {
				return nil, result.Err()
			}
			exprStruct := new(exprName)
			err = ion.Unmarshal(result.GetCurrentData(), &exprStruct)
			if err != nil {
				return nil, err
			}
			return exprStruct.Expr, nil
		})
		assert.NoError(t, searchErr)
		assert.Equal(t, fmt.Sprint("[", indexAttribute, "]"), searchRes.(string))
	})

	t.Run("Return empty when no records found", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		// Note : We are using a select * without specifying a where condition for the purpose of this test.
		//        However, we do not recommend using such a query in a normal/production context.
		query := fmt.Sprintf("SELECT * from %s", testTableName)
		selectRes, selectErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn)
		})
		assert.NoError(t, selectErr)
		assert.Equal(t, 0, selectRes.(int))
	})

	t.Run("Insert document", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record := TestTable{singleDocumentValue}
		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)

		insertResult, insertErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})

		assert.NoError(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, singleDocumentValue)
			if err != nil {
				return nil, err
			}
			if result.Next(txn) {
				decodedResult := ""
				decodedErr := ion.Unmarshal(result.GetCurrentData(), &decodedResult)
				if decodedErr != nil {
					return nil, decodedErr
				}
				return decodedResult, nil
			}
			return nil, result.Err()
		})
		assert.NoError(t, searchErr)
		assert.Equal(t, singleDocumentValue, searchResult.(string))
	})

	t.Run("Query table enclosed in quotes", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record := TestTable{singleDocumentValue}
		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)

		insertResult, insertErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})

		assert.NoError(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM \"%s\" WHERE %s = ?", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, singleDocumentValue)
			if err != nil {
				return nil, err
			}
			if result.Next(txn) {
				decodedResult := ""
				decodedErr := ion.Unmarshal(result.GetCurrentData(), &decodedResult)
				if decodedErr != nil {
					return nil, decodedErr
				}
				return decodedResult, nil
			}
			return nil, result.Err()
		})
		assert.NoError(t, searchErr)
		assert.Equal(t, singleDocumentValue, searchResult.(string))
	})

	t.Run("Insert multiple documents", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record1 := TestTable{multipleDocumentValue1}
		record2 := TestTable{multipleDocumentValue2}

		query := fmt.Sprintf("INSERT INTO %s <<?, ?>>", testTableName)
		insertResult, insertErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record1, record2)
		})

		assert.NoError(t, insertErr)
		assert.Equal(t, 2, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s IN (?,?)", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, multipleDocumentValue1, multipleDocumentValue2)
			if err != nil {
				return nil, err
			}
			results := make([]string, 0)
			for result.Next(txn) {
				decodedResult := "temp"
				decodedErr := ion.Unmarshal(result.GetCurrentData(), &decodedResult)
				if decodedErr != nil {
					return nil, decodedErr
				}
				results = append(results, decodedResult)
			}
			if result.Err() != nil {
				return nil, result.Err()
			}
			return results, nil
		})

		obtainedResults := searchResult.([]string)
		assert.NoError(t, searchErr)
		assert.True(t, contains(obtainedResults, multipleDocumentValue1))
		assert.True(t, contains(obtainedResults, multipleDocumentValue2))
	})

	t.Run("Delete single document", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		record := TestTable{singleDocumentValue}
		insertResult, insertErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})
		assert.NoError(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", testTableName, columnName)
		deleteResult, deleteErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), deleteQuery, txn, singleDocumentValue)
		})
		assert.NoError(t, deleteErr)
		assert.Equal(t, 1, deleteResult.(int))

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", testTableName)
		type rowCount struct {
			Count int `ion:"_1"`
		}
		countResult, countErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(countQuery)
			if err != nil {
				return nil, err
			}
			if !result.Next(txn) {
				return nil, result.Err()
			}
			countStruct := new(rowCount)
			err = ion.Unmarshal(result.GetCurrentData(), &countStruct)
			if err != nil {
				return nil, err
			}
			return countStruct.Count, nil
		})
		assert.NoError(t, countErr)
		assert.Equal(t, 0, countResult.(int))
	})

	t.Run("Delete all documents", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record1 := TestTable{multipleDocumentValue1}
		record2 := TestTable{multipleDocumentValue2}

		query := fmt.Sprintf("INSERT INTO %s <<?, ?>>", testTableName)
		insertResult, insertErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record1, record2)
		})

		assert.NoError(t, insertErr)
		assert.Equal(t, 2, insertResult.(int))

		deleteQuery := fmt.Sprintf("DELETE FROM %s", testTableName)
		deleteResult, deleteErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), deleteQuery, txn)
		})
		assert.NoError(t, deleteErr)
		assert.Equal(t, 2, deleteResult.(int))

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", testTableName)
		type rowCount struct {
			Count int `ion:"_1"`
		}
		countResult, countErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(countQuery)
			if err != nil {
				return nil, err
			}
			if !result.Next(txn) {
				return nil, result.Err()
			}
			countStruct := new(rowCount)
			err = ion.Unmarshal(result.GetCurrentData(), &countStruct)
			if err != nil {
				return nil, err
			}
			return countStruct.Count, nil
		})
		assert.NoError(t, countErr)
		assert.Equal(t, 0, countResult.(int))
	})

	t.Run("Test OCC Exception", func(t *testing.T) {
		type TestTable struct {
			Name string `ion:"Name"`
		}
		driver2, err := testBase.getDriver(ledger, 10, 0)
		require.NoError(t, err)
		record := TestTable{"dummy"}

		insertQuery := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		insertResult, insertErr := driver2.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), insertQuery, txn, record)
		})
		assert.NoError(t, insertErr)
		assert.Equal(t, insertResult.(int), 1)

		executeResult, err := driver2.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			_, err = txn.Execute(fmt.Sprintf("SELECT VALUE %s FROM %s", columnName, testTableName))
			assert.NoError(t, err)

			return driver2.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				return txn.Execute(fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName), 5)
			})

		})
		assert.Nil(t, executeResult)
		assert.IsType(t, &qldbsession.OccConflictException{}, err)
		_, ok := err.(*qldbsession.OccConflictException)
		assert.True(t, ok)
	})

	t.Run("Execution metrics for stream result", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		// Insert docs
		_, err = driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return txn.Execute(fmt.Sprintf("INSERT INTO %s << {'col': 1}, {'col': 2}, {'col': 3} >>", testTableName))
		})
		require.NoError(t, err)

		selectQuery := fmt.Sprintf("SELECT * FROM %s as a, %s as b, %s as c, %s as d, %s as e, %s as f",
			testTableName, testTableName, testTableName, testTableName, testTableName, testTableName)

		_, err = driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			result, err := txn.Execute(selectQuery)
			require.NoError(t, err)

			for result.Next(txn) {
				// IOUsage test
				ioUsage := result.GetConsumedIOs()
				require.NotNil(t, ioUsage)
				assert.True(t, *ioUsage.GetReadIOs() > 0)

				// TimingInformation test
				timingInfo := result.GetTimingInformation()
				require.NotNil(t, timingInfo)
				assert.True(t, *timingInfo.GetProcessingTimeMilliseconds() > 0)
			}
			return nil, nil
		})
		assert.NoError(t, err)
	})

	t.Run("Execution metrics for buffered result", func(t *testing.T) {
		driver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)
		defer driver.Shutdown(context.Background())
		defer cleanup(driver, testTableName)

		// Insert docs
		_, err = driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return txn.Execute(fmt.Sprintf("INSERT INTO %s << {'col': 1}, {'col': 2}, {'col': 3} >>", testTableName))
		})
		require.NoError(t, err)

		selectQuery := fmt.Sprintf("SELECT * FROM %s as a, %s as b, %s as c, %s as d, %s as e, %s as f",
			testTableName, testTableName, testTableName, testTableName, testTableName, testTableName)

		result, err := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			streamResult, err := txn.Execute(selectQuery)
			if err != nil {
				return nil, err
			}
			return txn.BufferResult(streamResult)
		})
		require.NoError(t, err)

		bufferedResult := result.(*BufferedResult)

		// IOUsage test
		ioUsage := bufferedResult.GetConsumedIOs()
		require.NotNil(t, ioUsage)
		assert.Equal(t, int64(1092), *ioUsage.GetReadIOs())

		// TimingInformation test
		timingInfo := bufferedResult.GetTimingInformation()
		require.NotNil(t, timingInfo)
		assert.True(t, *timingInfo.GetProcessingTimeMilliseconds() > 0)
	})

	t.Run("Insert and read Ion types", func(t *testing.T) {
		t.Run("struct", func(t *testing.T) {
			driver, err := testBase.getDriver(ledger, 10, 4)
			require.NoError(t, err)
			defer driver.Shutdown(context.Background())
			defer cleanup(driver, testTableName)

			type Anon struct {
				A, B int
			}
			parameterValue := Anon{42, 2}

			type TestTable struct {
				Name Anon `ion:"Name"`
			}
			parameter := TestTable{parameterValue}

			query := fmt.Sprintf("INSERT INTO %s ?", testTableName)
			executeResult, executeErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, parameter)
			})
			assert.NoError(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery, parameterValue)
				if err != nil {
					return nil, err
				}
				if !result.Next(txn) {
					return nil, result.Err()
				}
				ionReceiver := new(Anon)
				err = ion.Unmarshal(result.GetCurrentData(), &ionReceiver)
				if err != nil {
					return nil, err
				}
				return ionReceiver, nil
			})
			assert.NoError(t, searchErr)
			assert.Equal(t, &parameterValue, searchResult.(*Anon))
		})

		testInsertCommon := func(testName, inputQuery, searchQuery string, parameterValue, ionReceiver, parameter interface{}) {
			t.Run(testName, func(t *testing.T) {
				driver, err := testBase.getDriver(ledger, 10, 4)
				require.NoError(t, err)
				defer driver.Shutdown(context.Background())
				defer cleanup(driver, testTableName)

				executeResult, executeErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
					return executeWithParam(context.Background(), inputQuery, txn, parameter)
				})
				assert.NoError(t, executeErr)
				assert.Equal(t, 1, executeResult.(int))
				searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
					result, err := txn.Execute(searchQuery, parameterValue)
					if err != nil {
						return nil, err
					}
					if !result.Next(txn) {
						return nil, result.Err()
					}
					err = ion.Unmarshal(result.GetCurrentData(), ionReceiver)
					if err != nil {
						return nil, err
					}
					return ionReceiver, nil
				})
				assert.NoError(t, searchErr)
				switch actualVal := searchResult.(type) {
				case *bool:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *int:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *float32:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *float64:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *[]int:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						fmt.Println(*actualVal)
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *[]string:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				default:
					t.Errorf("Could not find type")
				}

			})
		}

		// boolean
		type TestTableBoolean struct {
			Name bool `ion:"Name"`
		}
		boolParam := true
		testInsertCommon("boolean",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			boolParam,
			new(bool),
			TestTableBoolean{boolParam},
		)

		// integer
		type TestTableInt struct {
			Name int `ion:"Name"`
		}
		intParam := 5
		testInsertCommon("integer",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			intParam,
			new(int),
			TestTableInt{intParam},
		)

		// float32
		type TestTableFloat32 struct {
			Name float32 `ion:"Name"`
		}
		var float32Param float32
		float32Param = math.MaxFloat32
		testInsertCommon("float32",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float32Param,
			new(float32),
			TestTableFloat32{float32Param},
		)

		// float64
		type TestTableFloat64 struct {
			Name float64 `ion:"Name"`
		}
		var float64Param float64
		float64Param = math.MaxFloat64
		testInsertCommon("float64",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float64Param,
			new(float64),
			TestTableFloat64{float64Param},
		)

		// int slice
		type TestTableSlice struct {
			Name []int `ion:"Name"`
		}
		parameterValue := []int{2, 3, 4}
		testInsertCommon("slice int",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			parameterValue,
			&[]int{},
			TestTableSlice{parameterValue},
		)

		// string slice
		type TestTableSliceString struct {
			Name []string `ion:"Name"`
		}
		stringParam := []string{"Hello", "How", "Are"}
		testInsertCommon("slice string",
			fmt.Sprintf("INSERT INTO %s ?", testTableName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			stringParam,
			&[]string{},
			TestTableSliceString{stringParam},
		)

	})

	t.Run("Update Ion types", func(t *testing.T) {
		updateDriver, err := testBase.getDriver(ledger, 10, 4)
		require.NoError(t, err)

		type TestTable struct {
			Name int `ion:"Name"`
		}
		parameter := TestTable{1}

		insertQuery := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		_, err = updateDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), insertQuery, txn, parameter)
		})
		require.NoError(t, err)

		testUpdateCommon := func(testName, inputQuery, searchQuery string, parameterValue, ionReceiver, parameter interface{}) {
			t.Run(testName, func(t *testing.T) {
				driver, err := testBase.getDriver(ledger, 10, 4)
				require.NoError(t, err)
				defer driver.Shutdown(context.Background())

				executeResult, executeErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
					return executeWithParam(context.Background(), inputQuery, txn, parameter)
				})
				assert.NoError(t, executeErr)
				assert.Equal(t, 1, executeResult.(int))
				searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
					result, err := txn.Execute(searchQuery, parameterValue)
					if err != nil {
						return nil, err
					}
					if !result.Next(txn) {
						return nil, result.Err()
					}
					err = ion.Unmarshal(result.GetCurrentData(), ionReceiver)
					if err != nil {
						return nil, err
					}
					return ionReceiver, nil
				})
				assert.NoError(t, searchErr)
				switch actualVal := searchResult.(type) {
				case *bool:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *int:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *float32:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *float64:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *[]int:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						fmt.Println(*actualVal)
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				case *[]string:
					if !reflect.DeepEqual(parameterValue, *actualVal) {
						t.Errorf("expected %v, got %v", parameterValue, reflect.ValueOf(*actualVal))
					}
				default:
					t.Errorf("Could not find type")
				}
			})
		}

		// boolean
		boolParam := true
		testUpdateCommon("boolean",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			boolParam,
			new(bool),
			boolParam,
		)

		// integer
		intParam := 5
		testUpdateCommon("integer",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			intParam,
			new(int),
			intParam,
		)

		// float32
		var float32Param float32
		float32Param = math.MaxFloat32
		testUpdateCommon("float32",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float32Param,
			new(float32),
			float32Param,
		)

		// float64
		var float64Param float64
		float64Param = math.MaxFloat64
		testUpdateCommon("float64",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float64Param,
			new(float64),
			float64Param,
		)

		// int slice
		parameterValue := []int{2, 3, 4}
		testUpdateCommon("slice int",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			parameterValue,
			&[]int{},
			parameterValue,
		)

		// string slice
		stringParam := []string{"Hello", "How", "Are"}
		testUpdateCommon("slice string",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			stringParam,
			&[]string{},
			stringParam,
		)

		t.Run("nil", func(t *testing.T) {
			driver, err := testBase.getDriver(ledger, 10, 4)
			require.NoError(t, err)
			defer driver.Shutdown(context.Background())

			query := fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName)
			executeResult, executeErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, nil)
			})
			assert.NoError(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s IS NULL", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery)
				if err != nil {
					return nil, err
				}
				if !result.Next(txn) {
					return nil, result.Err()
				}
				ionReceiver := ""
				err = ion.Unmarshal(result.GetCurrentData(), &ionReceiver)
				if err != nil {
					return nil, err
				}
				return ionReceiver, nil
			})
			assert.NoError(t, searchErr)
			assert.Equal(t, searchResult.(string), "")

		})

		t.Run("struct", func(t *testing.T) {
			driver, err := testBase.getDriver(ledger, 10, 4)
			require.NoError(t, err)
			defer driver.Shutdown(context.Background())
			defer cleanup(driver, testTableName)

			type Anon struct {
				A, B int
			}
			parameterValue := Anon{42, 2}

			query := fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName)
			executeResult, executeErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, parameterValue)
			})
			assert.NoError(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery, parameterValue)
				if err != nil {
					return nil, err
				}
				if !result.Next(txn) {
					return nil, result.Err()
				}
				ionReceiver := new(Anon)
				err = ion.Unmarshal(result.GetCurrentData(), &ionReceiver)
				if err != nil {
					return nil, err
				}
				return ionReceiver, nil
			})
			assert.NoError(t, searchErr)
			assert.Equal(t, &parameterValue, searchResult.(*Anon))
		})

	})

	t.Run("Delete Table that does not exist", func(t *testing.T) {
		query := "DELETE FROM NonExistentTable"
		result, err := qldbDriver.Execute(context.Background(), func(txn Transaction) (interface{}, error) {
			return txn.Execute(query)
		})
		assert.Nil(t, result)
		assert.IsType(t, &qldbsession.BadRequestException{}, err)
		_, ok := err.(*qldbsession.BadRequestException)
		assert.True(t, ok)
	})

	// teardown
	qldbDriver.Shutdown(context.Background())
	testBase.deleteLedger(t)
}
