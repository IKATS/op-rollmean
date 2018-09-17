# Rollmean Operator

This IKATS operator implements a moving average. A resample can be performed if requested.


## Input and parameters

This operator only takes one input of the functional type `DS_name`.

It also takes upto 4 inputs from the user :

- **Window period** : Duration of the moving window (in ms).
- **Window size** : Mutually exclusive with **Window period**. Size (in points) of the moving window. This is the number of observations used for calculating the statistic.
- **Alignment** : Result alignement :
    - 1: Left
    - 2: Center (default)
    - 3: Right

## Outputs

The operator has one output :

 - **TS_list** : the resulting TS List

## Principle

Compute the rollmean on TS data provided

This algorithm needs:
* a TS with a reference period calculated (using quality stats algorithm for exampl)
* a window range (in number of points or in time (ms))
* an alignment method for the output

>**Warnings:**
>* The TS must not contain any hole (an interpolation may be applied before calling this algorithm).
>* The result will be altered and may not represent the real behaviour of the rollmean algorithm
>* The metadata 'qual_ref_period' shall be defined for all timeseries of the dataset (can be porformed by Quality stats algorithm)

_**About alignment:**_

Assuming
* K is the window length (in number of points)

Then
* Left alignment corresponds to the beginning of the calculation sliding window
* Center alignmentcorresponds to the middlz of the calculation sliding window - timestamp will be shifted by floor((K-1)/2)
* Right alignment corresponds to the end of the calculation sliding window - timestamp will be shifted by (K-1)


_**About size of the final TS**_

Due to the mean, we have less points in the result TS than the original TS
Assuming
    * K is the window length (in number of points)
    * N is the number of points of the TS
the length of the new TS will be N-K

## Example:


Applying a rollmean on a TS stored in ts1_data with a window having 2 points
The result will be left-aligned

To understand what is done, given this ts_data:

| Timestamp | Value |
|-----------|-------|
| 1000      | 1     |
| 2000      | 10    |
| 3000      | 20    |
| 4000      | 5     |
| 5000      | 8     |
| 6000      | 2     |

We want to apply a rollmean with a window equal to 2 points:
* we take the first window [1000;2000]
* the mean of points is (sum of points in window divided by the size of the window): (1 + 10) / 2 = 5.5
* Now the alignment is left so the value 5.5 will be assigned to the timestamp 1000 (would be assigned to timestamp 2000 for a right alignement)
* we shift the window by one point [2000;3000]
* Start again until the TS is fully parsed


