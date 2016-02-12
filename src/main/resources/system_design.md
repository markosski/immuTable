
# System Design

## Data Storage on FS

- Indexing process
Indexer is given a source file which is a delimited csv type file. Indexer will process one line at a time
working on each field separately and building index for each. Indexer will be also given information about data types.

Let's consider the following records:
34 | Marcin | NY | 10538 - CID = 0
32 | Melissa | CT | 12345 - CID = 1

# Core concepts
- Algebraic Operators
Select = (Vec[oid, val], T, T) => Vec[oid] - select range on intermediate
Select = (Vec[val], T, T) => Vec[oid] - select range on column, oid is virtual
Select = (Vec[oid, val], T) => Vec[oid] - select exact
Select = (Vec[val], T) => Vec[oid] - select exact, oid is virtual
Create = (Vec[oid], Vec[val]) => Vec[oid, val] - create intermediate with oid
Sum = Vec[oid, val] => val
Sum = Vec[val] => val
Avg = Vec[oid, val] => val
Avg = Vec[val] => val
...

trait CVector
trait RangeCVector

Q: How does this translate to the column types that are already created?

- Intermediate Columns (IC)
Intermediates are in-memory, temporary data sources that are used as inputs to next operators in query execution plan.
Since similar query would need to recreate over again IC data structure we could store them for future use. LRU could be
used here as algorithm to manage it.

- Column Cracking
Column cracking technique is ongoing optimizing column data layout. It will order data only based on used predicates.

Column Crack:

| oid | age |
|-----------|
|  25 | 18  |
|  45 | 18  |
|  31 | 18  |
|  8  | 19  |
|  1  | 19  |
|  13 | 20  |

Column Crack Index:

| age | pos |
|-----------|
| 18  | 0   |
| 19  | 3   |
| 20  | 5   |


# Proposed Data Format

## Dense
Fixed length data only.
This data format is a essentially 1 dim array with values. In this format type array keys represent OID.
This format is optimized for low cardinality data.
| n-bytes |

Pros:
- most efficient for small data types up to 4 byte integers
- fast full scans for
- may be good for high cardinality small values
- fast construction

Cons:
- not great for bigger data types due to higher IO demands
- full scan always

## DenseSorted
Data files is ordered ascending. Addidional file is maintained that stored OIDs and value lookup for range queries.

Pros:
- no need for full scan
- can work well with high cardinality data that would not benefit from run-lenght encoding.
    Can use full inverted index or binned inverted index

## RunLengthSorted
Numerical data only.
This data format is more optimized for numerical low cardinality (e.g. age, zip code, state).
For high cardinality use DenseSorted. It stores only unique values
in sorted order in run-length encoding. There is also a a file *.oid that contains OID.
Location of starting byte in heap file is calculated by index and repeat value in run-length encoding. End byte is inferred
based on data length and repeat value.
| n-bytes (value) | *4 bytes (repeat) | -> | 4 bytes heap (materialized OIDs)|

Pros:
- cheap counts for any value
- cheap construction of itermediate vector
- *count field may be ignores as ref to heap can be easily used to get counts
- may take less space than Dense format e.g. zip code (5 bytes) - at 100mil records 380MB, vs 480MB for Dense

Cons:
- slow construction due to sorting
- may take much more space in contrast with Dense for smaller data types.

## VarLength
Var length data only.
This is an extension of Dense format where we store 1 column that represents start byte. End byte is calculated by checking
the value of next 4 bytes.
| 4 bytes (offset) | -> | 4 bytes heap (unique values) |

Pros:
- stores only unique var length values in heap file hence offset value is a representation of the real value
- operations can be done on offsets that represent value in the heap file
- offers similar characteristics to Dense format

Cons:
- In most cases will take more space than Dense format
- slow construction due to checking for duplicates
- scan may be slow due to checking value at each iteration
- needs extra hash structure to convert between values to ref id in which case during scan we can treat ref is as value

## File Types
.fix - full inverted index
.bix - binned inverted index
.dat - dense column attributes
.oid - dense column OIDs
.var - varlength data
.bof - byte offsets data

## Calculate Space for AME sample table

- age - Dense, 1 byte, 25MB
- income - RunLength, 4 byte, 100MB
- marital - Dense, 1 byte 25MB
- ethnicity - Dense, 1 byte 25MB
- gender - Dense, 1 byte, 25MB
- bool_models - RunLength, 4 byte, 100MB
- model x 20 - Dense, 4 byte, 100MB = 2000MB
- geo_unit - RunLength, 5 byte, 120MB
- in_market_id - Dense, 8 byte, 200MB
- latitude - Dense, 8 byte, 200MB
- longitude - Dense, 8 byte, 200MB
- geo_unit_lat - RunLength, 8 byte, 100MB
- geo_unit_lng - RunLength, 8 byte, 100MB

extra
- region - RunLength, 6 bytes, 100MB
- area - RunLength, 6 bytes, 100MB
- dma - RunLength, 6 bytes, 100MB
- dealer - RunLength, 6 bytes, 100MB
- region_ext - RunLength, 6 bytes, 100MB
- area_ext - RunLength, 6 bytes, 100MB
- dma_ext - RunLength, 6 bytes, 100MB
- dealer_ext - RunLength, 6 bytes, 100MB

Total: About 4GB including 20 models and geo data, and geo centroids - no joins needed






