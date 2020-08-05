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

package integration

import (
	"context"
	"fmt"
	"math"
	"qldbdriver/qldbdriver"
	"reflect"
	"testing"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/stretchr/testify/assert"
)

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func cleanup(driver *qldbdriver.QLDBDriver, testTableName string) {
	driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute(fmt.Sprintf("DELETE FROM %s", testTableName))
	})
}

func TestStatementExecution(t *testing.T) {
	//setup
	testBase := createTestBase()
	testBase.deleteLedger(t)
	testBase.createLedger(t)

	qldbDriver := testBase.getDriver(ledger, 10, 4)
	qldbDriver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute(fmt.Sprintf("CREATE TABLE %s", testTableName))
	})

	executeWithParam := func(ctx context.Context, query string, txn qldbdriver.Transaction, parameters ...interface{}) (interface{}, error) {
		result, err := txn.Execute(query, parameters...)
		if err != nil {
			return nil, err
		}
		count := 0
		for result.HasNext() {
			_, err := result.Next(txn)
			if err != nil {
				return nil, err
			}
			count++
		}
		return count, nil
	}

	t.Run("Drop existing table", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())

		createTableName := "GoIntegrationTestCreateTable"
		createTableQuery := fmt.Sprintf("CREATE TABLE %s", createTableName)

		executeResult, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), createTableQuery, txn)
		})
		assert.Nil(t, err)
		actualResult := executeResult.(int)
		assert.Equal(t, 1, actualResult)

		tables, err := driver.GetTableNames(context.Background())
		assert.Nil(t, err)
		assert.True(t, contains(tables, createTableName))

		dropTableQuery := fmt.Sprintf("DROP TABLE %s", createTableName)
		dropResult, droperr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), dropTableQuery, txn)
		})
		assert.Nil(t, droperr)
		assert.Equal(t, 1, dropResult.(int))
	})

	t.Run("List tables", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		tables, err := driver.GetTableNames(context.Background())
		assert.Nil(t, err)
		assert.True(t, contains(tables, testTableName))
	})

	t.Run("Create table that exists", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		query := fmt.Sprintf("CREATE TABLE %s", testTableName)

		result, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return txn.Execute(query)
		})
		assert.Nil(t, result)
		assert.IsType(t, &qldbsession.BadRequestException{}, err)
		_, ok := err.(*qldbsession.BadRequestException)
		assert.True(t, ok)
	})

	t.Run("Create index", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		indexQuery := fmt.Sprintf("CREATE INDEX ON %s (%s)", testTableName, indexAttribute)
		indexResult, indexErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), indexQuery, txn)
		})
		assert.Nil(t, indexErr)
		assert.Equal(t, 1, indexResult.(int))

		searchQuery := "SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE' "
		searchQuery += fmt.Sprintf("AND name = '%s'", testTableName)

		type exprName struct {
			Expr string `ion:"expr"`
		}

		searchRes, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery)
			if err != nil {
				return nil, err
			}
			ionBinary, err := result.Next(txn)
			if err != nil {
				return nil, err
			}
			exprStruct := new(exprName)
			ionErr := ion.Unmarshal(ionBinary, &exprStruct)
			if ionErr != nil {
				return nil, ionErr
			}
			return exprStruct.Expr, nil
		})
		assert.Nil(t, searchErr)
		assert.Equal(t, fmt.Sprint("[", indexAttribute, "]"), searchRes.(string))
	})

	t.Run("Return empty when no records found", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		query := fmt.Sprintf("SELECT * from %s", testTableName)
		selectRes, selectErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn)
		})
		assert.Nil(t, selectErr)
		assert.Equal(t, 0, selectRes.(int))
	})

	t.Run("Insert document", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record := TestTable{singleDocumentValue}
		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)

		insertResult, insertErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})

		assert.Nil(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, singleDocumentValue)
			if err != nil {
				return nil, err
			}
			if result.HasNext() {
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				decodedResult := ""
				decodedErr := ion.Unmarshal(ionBinary, &decodedResult)
				if decodedErr != nil {
					return nil, err
				}
				return decodedResult, nil
			}
			return nil, nil
		})
		assert.Nil(t, searchErr)
		assert.Equal(t, singleDocumentValue, searchResult.(string))
	})

	t.Run("Query table enclosed in quotes", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record := TestTable{singleDocumentValue}
		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)

		insertResult, insertErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})

		assert.Nil(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM \"%s\" WHERE %s = ?", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, singleDocumentValue)
			if err != nil {
				return nil, err
			}
			if result.HasNext() {
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				decodedResult := ""
				decodedErr := ion.Unmarshal(ionBinary, &decodedResult)
				if decodedErr != nil {
					return nil, err
				}
				return decodedResult, nil
			}
			return nil, nil
		})
		assert.Nil(t, searchErr)
		assert.Equal(t, singleDocumentValue, searchResult.(string))
	})

	t.Run("Insert multiple documents", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record1 := TestTable{multipleDocumentValue1}
		record2 := TestTable{multipleDocumentValue2}

		query := fmt.Sprintf("INSERT INTO %s <<?, ?>>", testTableName)
		insertResult, insertErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record1, record2)
		})

		assert.Nil(t, insertErr)
		assert.Equal(t, 2, insertResult.(int))

		searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s IN (?,?)", columnName, testTableName, columnName)
		searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(searchQuery, multipleDocumentValue1, multipleDocumentValue2)
			if err != nil {
				return nil, err
			}
			results := make([]string, 0)
			for result.HasNext() {
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				decodedResult := "temp"
				decodedErr := ion.Unmarshal(ionBinary, &decodedResult)
				if decodedErr != nil {
					return nil, err
				}
				results = append(results, decodedResult)
			}
			return results, nil
		})

		obtainedResults := searchResult.([]string)
		assert.Nil(t, searchErr)
		assert.True(t, contains(obtainedResults, multipleDocumentValue1))
		assert.True(t, contains(obtainedResults, multipleDocumentValue2))
	})

	t.Run("Delete single document", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		query := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		record := TestTable{singleDocumentValue}
		insertResult, insertErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record)
		})
		assert.Nil(t, insertErr)
		assert.Equal(t, 1, insertResult.(int))

		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", testTableName, columnName)
		deleteResult, deleteErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), deleteQuery, txn, singleDocumentValue)
		})
		assert.Nil(t, deleteErr)
		assert.Equal(t, 1, deleteResult.(int))

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", testTableName)
		type rowCount struct {
			Count int `ion:"_1"`
		}
		countResult, countErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(countQuery)
			if err != nil {
				return nil, err
			}
			ionBinary, err := result.Next(txn)
			if err != nil {
				return nil, err
			}
			countStruct := new(rowCount)
			ionErr := ion.Unmarshal(ionBinary, &countStruct)
			if ionErr != nil {
				return nil, ionErr
			}
			return countStruct.Count, nil
		})
		assert.Nil(t, countErr)
		assert.Equal(t, 0, countResult.(int))
	})

	t.Run("Delete all documents", func(t *testing.T) {
		driver := testBase.getDriver(ledger, 10, 4)
		defer driver.Close(context.Background())
		defer cleanup(driver, testTableName)

		type TestTable struct {
			Name string `ion:"Name"`
		}

		record1 := TestTable{multipleDocumentValue1}
		record2 := TestTable{multipleDocumentValue2}

		query := fmt.Sprintf("INSERT INTO %s <<?, ?>>", testTableName)
		insertResult, insertErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), query, txn, record1, record2)
		})

		assert.Nil(t, insertErr)
		assert.Equal(t, 2, insertResult.(int))

		deleteQuery := fmt.Sprintf("DELETE FROM %s", testTableName)
		deleteResult, deleteErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), deleteQuery, txn)
		})
		assert.Nil(t, deleteErr)
		assert.Equal(t, 2, deleteResult.(int))

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", testTableName)
		type rowCount struct {
			Count int `ion:"_1"`
		}
		countResult, countErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			result, err := txn.Execute(countQuery)
			if err != nil {
				return nil, err
			}
			ionBinary, err := result.Next(txn)
			if err != nil {
				return nil, err
			}
			countStruct := new(rowCount)
			ionErr := ion.Unmarshal(ionBinary, &countStruct)
			if ionErr != nil {
				return nil, ionErr
			}
			return countStruct.Count, nil
		})
		assert.Nil(t, countErr)
		assert.Equal(t, 0, countResult.(int))
	})

	t.Run("Test OCC Exception", func(t *testing.T) {
		type TestTable struct {
			Name string `ion:"Name"`
		}
		driver2 := testBase.getDriver(ledger, 10, 0)
		record := TestTable{"dummy"}

		insertQuery := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		insertResult, insertErr := driver2.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), insertQuery, txn, record)
		})
		assert.Nil(t, insertErr)
		assert.Equal(t, insertResult.(int), 1)

		executeResult, err := driver2.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			txn.Execute(fmt.Sprintf("SELECT VALUE %s FROM %s", columnName, testTableName))

			return driver2.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				return txn.Execute(fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName), 5)
			})

		})
		assert.Nil(t, executeResult)
		assert.IsType(t, &qldbsession.OccConflictException{}, err)
		_, ok := err.(*qldbsession.OccConflictException)
		assert.True(t, ok)
	})

	t.Run("Insert and read Ion types", func(t *testing.T) {
		t.Run("struct", func(t *testing.T) {
			driver := testBase.getDriver(ledger, 10, 4)
			defer driver.Close(context.Background())
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
			executeResult, executeErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, parameter)
			})
			assert.Nil(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery, parameterValue)
				if err != nil {
					return nil, err
				}
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				ionReceiver := new(Anon)
				ionErr := ion.Unmarshal(ionBinary, &ionReceiver)
				if ionErr != nil {
					return nil, ionErr
				}
				return ionReceiver, nil
			})
			assert.Nil(t, searchErr)
			assert.Equal(t, &parameterValue, searchResult.(*Anon))

		})

		testInsertCommon := func(testName, inputQuery, searchQuery string, parameterValue, ionReceiver, parameter interface{}) {
			t.Run(testName, func(t *testing.T) {
				driver := testBase.getDriver(ledger, 10, 4)
				defer driver.Close(context.Background())
				defer cleanup(driver, testTableName)

				executeResult, executeErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
					return executeWithParam(context.Background(), inputQuery, txn, parameter)
				})
				assert.Nil(t, executeErr)
				assert.Equal(t, 1, executeResult.(int))
				searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
					result, err := txn.Execute(searchQuery, parameterValue)
					if err != nil {
						return nil, err
					}
					ionBinary, err := result.Next(txn)
					if err != nil {
						return nil, err
					}
					ionErr := ion.Unmarshal(ionBinary, ionReceiver)
					if ionErr != nil {
						return nil, ionErr
					}
					return ionReceiver, nil
				})
				assert.Nil(t, searchErr)
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

		//boolean
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

		//integer
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

		//float32
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

		//float64
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

		//int slice
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

		//string slice
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
		updateDriver := testBase.getDriver(ledger, 10, 4)

		type TestTable struct {
			Name int `ion:"Name"`
		}
		parameter := TestTable{1}

		insertQuery := fmt.Sprintf("INSERT INTO %s ?", testTableName)
		updateDriver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return executeWithParam(context.Background(), insertQuery, txn, parameter)
		})

		testUpdateCommon := func(testName, inputQuery, searchQuery string, parameterValue, ionReceiver, parameter interface{}) {
			t.Run(testName, func(t *testing.T) {
				driver := testBase.getDriver(ledger, 10, 4)
				defer driver.Close(context.Background())

				executeResult, executeErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
					return executeWithParam(context.Background(), inputQuery, txn, parameter)
				})
				assert.Nil(t, executeErr)
				assert.Equal(t, 1, executeResult.(int))
				searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
					result, err := txn.Execute(searchQuery, parameterValue)
					if err != nil {
						return nil, err
					}
					ionBinary, err := result.Next(txn)
					if err != nil {
						return nil, err
					}
					ionErr := ion.Unmarshal(ionBinary, ionReceiver)
					if ionErr != nil {
						return nil, ionErr
					}
					return ionReceiver, nil
				})
				assert.Nil(t, searchErr)
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

		//boolean
		boolParam := true
		testUpdateCommon("boolean",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			boolParam,
			new(bool),
			boolParam,
		)

		//integer
		intParam := 5
		testUpdateCommon("integer",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			intParam,
			new(int),
			intParam,
		)

		//float32
		var float32Param float32
		float32Param = math.MaxFloat32
		testUpdateCommon("float32",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float32Param,
			new(float32),
			float32Param,
		)

		//float64
		var float64Param float64
		float64Param = math.MaxFloat64
		testUpdateCommon("float64",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			float64Param,
			new(float64),
			float64Param,
		)

		//int slice
		parameterValue := []int{2, 3, 4}
		testUpdateCommon("slice int",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			parameterValue,
			&[]int{},
			parameterValue,
		)

		//string slice
		stringParam := []string{"Hello", "How", "Are"}
		testUpdateCommon("slice string",
			fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName),
			fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName),
			stringParam,
			&[]string{},
			stringParam,
		)

		t.Run("nil", func(t *testing.T) {
			driver := testBase.getDriver(ledger, 10, 4)
			defer driver.Close(context.Background())

			query := fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName)
			executeResult, executeErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, nil)
			})
			assert.Nil(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s IS NULL", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery)
				if err != nil {
					return nil, err
				}
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				ionReceiver := ""
				ionErr := ion.Unmarshal(ionBinary, &ionReceiver)
				if ionErr != nil {
					return nil, ionErr
				}
				return ionReceiver, nil
			})
			assert.Nil(t, searchErr)
			assert.Equal(t, searchResult.(string), "")

		})

		t.Run("struct", func(t *testing.T) {
			driver := testBase.getDriver(ledger, 10, 4)
			defer driver.Close(context.Background())
			defer cleanup(driver, testTableName)

			type Anon struct {
				A, B int
			}
			parameterValue := Anon{42, 2}

			query := fmt.Sprintf("UPDATE %s SET %s = ?", testTableName, columnName)
			executeResult, executeErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				return executeWithParam(context.Background(), query, txn, parameterValue)
			})
			assert.Nil(t, executeErr)
			assert.Equal(t, 1, executeResult.(int))

			searchQuery := fmt.Sprintf("SELECT VALUE %s FROM %s WHERE %s = ?", columnName, testTableName, columnName)
			searchResult, searchErr := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
				result, err := txn.Execute(searchQuery, parameterValue)
				if err != nil {
					return nil, err
				}
				ionBinary, err := result.Next(txn)
				if err != nil {
					return nil, err
				}
				ionReceiver := new(Anon)
				ionErr := ion.Unmarshal(ionBinary, &ionReceiver)
				if ionErr != nil {
					return nil, ionErr
				}
				return ionReceiver, nil
			})
			assert.Nil(t, searchErr)
			assert.Equal(t, &parameterValue, searchResult.(*Anon))

		})

	})

	t.Run("Delete Table that does not exist", func(t *testing.T) {
		query := "DELETE FROM NonExistentTable"
		result, err := qldbDriver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return txn.Execute(query)
		})
		assert.Nil(t, result)
		assert.IsType(t, &qldbsession.BadRequestException{}, err)
		_, ok := err.(*qldbsession.BadRequestException)
		assert.True(t, ok)
	})

	//teardown
	qldbDriver.Close(context.Background())
	testBase.deleteLedger(t)
}
