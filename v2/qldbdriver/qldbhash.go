/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package qldbdriver

import (
	"crypto/sha256"

	"github.com/amzn/ion-go/ion"
	ionhash "github.com/amzn/ion-hash-go"
)

const hashSize = 32

type qldbHash struct {
	hash []byte
}

func toQLDBHash(value interface{}) (*qldbHash, error) {
	ionValue, err := ion.MarshalBinary(value)
	if err != nil {
		return nil, err
	}
	ionReader := ion.NewReaderBytes(ionValue)
	hashReader, err := ionhash.NewHashReader(ionReader, ionhash.NewCryptoHasherProvider(ionhash.SHA256))
	if err != nil {
		return nil, err
	}
	for hashReader.Next() {
		// Read over value
	}
	hash, err := hashReader.Sum(nil)
	if err != nil {
		return nil, err
	}
	return &qldbHash{hash}, nil
}

func (thisHash *qldbHash) dot(thatHash *qldbHash) (*qldbHash, error) {
	concatenated, err := joinHashesPairwise(thisHash.hash, thatHash.hash)
	if err != nil {
		return nil, err
	}

	newHash := sha256.Sum256(concatenated)
	return &qldbHash{newHash[:]}, nil
}

func joinHashesPairwise(h1 []byte, h2 []byte) ([]byte, error) {
	if len(h1) == 0 {
		return h2, nil
	}
	if len(h2) == 0 {
		return h1, nil
	}

	compare, err := hashComparator(h1, h2)
	if err != nil {
		return nil, err
	}

	var concatenated []byte
	if compare < 0 {
		concatenated = append(h1, h2...)
	} else {
		concatenated = append(h2, h1...)
	}
	return concatenated, nil
}

func hashComparator(h1 []byte, h2 []byte) (int16, error) {
	if len(h1) != hashSize || len(h2) != hashSize {
		return 0, &qldbDriverError{"invalid hash"}
	}
	for i := range h1 {
		// Reverse index for little endianness
		index := hashSize - 1 - i

		// Handle byte being unsigned and overflow
		h1Int := int16(h1[index])
		h2Int := int16(h2[index])
		if h1Int > 127 {
			h1Int = 0 - (256 - h1Int)
		}
		if h2Int > 127 {
			h2Int = 0 - (256 - h2Int)
		}

		difference := h1Int - h2Int
		if difference != 0 {
			return difference, nil
		}
	}
	return 0, nil
}
