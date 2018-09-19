# Rollmean Operator

This IKATS operator implements a *moving average*.

Review#495 (FTA): This sentence is wrong
A resample can be performed if requested.

## Input and parameters

This operator only takes one input of the functional type `DS_name`.

It also takes up to 4 inputs from the user:

- **Window period**: Duration of the moving window (in milliseconds).
- **Window size**: Mutually exclusive with **Window period**. Size (in points) of the moving window. This is the number of observations used for calculating the statistic.
- **Alignment**: Result alignment:
  - `1`: Left
  - `2`: Center (*default*)
  - `3`: Right

## Outputs

The operator has one output:

- **TS_list**: The resulting list of time series

## Principle

Compute the rollmean on TS data provided

This operator needs:

- a TS with a reference period calculated (using *Quality Indicators* operator), aka: `qual_ref_period` metadata
- a window range (in number of points or in time (milliseconds))
- an alignment method for the output

### Warnings

- the TS must not contain any hole (an interpolation may be applied before calling this operator)
- the result will be altered and may not represent the real behaviour of the rollmean operator
- the metadata `qual_ref_period` shall be defined for all time series of the dataset (can be performed by *Quality Indicators* operator)

 #Review#495: (FTA) "the result will be altered". Such a sentence is pretty scary! explain
 #Review#495: (FTA) i don't see any explanation about the ceil action when window period doesn't match an exact number of points

### Consideration about alignment

Assuming

- `K` is the window length (in number of points)

Then

- *Left* alignment corresponds to the beginning of the calculation sliding window
- *Center* alignment corresponds to the middle of the calculation sliding window - timestamp will be shifted by `floor((K-1)/2)`
- *Right* alignment corresponds to the end of the calculation sliding window - timestamp will be shifted by `(K-1)`

### Consideration about the size of the final TS

Due to the mean operation, we have less points in the result TS than the original TS

Assuming

- `K` is the window length (in number of points)
- `N` is the number of points of the TS

then the length of the new TS will be `N-K`

## Example

Applying rollmean on a TS stored in *ts1_data* with a window having 2 points
The result will be left-aligned

To understand what is done, given this data:

| Timestamp | Value |
| --------- | ----- |
| 1000      | 1     |
| 2000      | 10    |
| 3000      | 20    |
| 4000      | 5     |
| 5000      | 8     |
| 6000      | 2     |

We want to apply rollmean with a window equal to 2 points:

- we take the first window `[1000;2000]`
- the mean of points is (sum of points in window divided by the size of the window): `(1 + 10) / 2 = 5.5`
- now the alignment is left so the value `5.5` will be assigned to the timestamp `1000` (would be assigned to timestamp `2000` for a *right alignment*)
- we shift the window by one point `[2000;3000]`
- start again until the TS is fully parsed
