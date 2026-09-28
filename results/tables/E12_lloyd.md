| Setting | lloyd vs | pairs | lloyd better | mean gain | median gain | one-sided Wilcoxon p |
|---|---|---|---|---|---|---|
| static real | norm | 120 | 38% | +0.0004 | -0.0048 | 0.75 |
| static real | objective | 120 | 43% | +0.0018 | -0.0029 | 0.4 |
| drifting (SynDrift) | norm | 30 | 73% | +0.1165 | +0.0153 | 2.2e-05 |
| drifting (SynDrift) | objective | 30 | 67% | +0.0276 | +0.0047 | 0.00044 |
| evolving real | norm | 100 | 75% | +0.0342 | +0.0284 | 5.6e-06 |
| evolving real | objective | 100 | 66% | +0.0214 | +0.0164 | 0.00033 |
| non-stationary (drift + evolving) | norm | 130 | 75% | +0.0532 | +0.0225 | 7.4e-09 |
| non-stationary (drift + evolving) | objective | 130 | 66% | +0.0229 | +0.0103 | 6.6e-06 |
| all | norm | 250 | 57% | +0.0278 | +0.0072 | 2.2e-06 |
| all | objective | 250 | 55% | +0.0127 | +0.0045 | 0.00011 |

| Dataset / setting | lloyd | objective | top-k magnitude | excess reduction vs magnitude |
|---|---|---|---|---|
| SynDrift (drifting (SynDrift)) | 1.0183 | 1.0459 | 1.1347 | 86% |
| Letter (evolving real) | 1.0843 | 1.0987 | 1.1055 | 20% |
| NSL-KDD (evolving real) | 1.1455 | 1.1239 | 1.1689 | 14% |
| Pen-Digits (evolving real) | 1.0379 | 1.1405 | 1.1296 | 71% |
| Shuttle (evolving real) | 1.0098 | 1.0003 | 1.0103 | 5% |
| Letter (static real) | 1.0584 | 1.0541 | 1.0520 | -12% |
| NSL-KDD (static real) | 1.0754 | 1.0859 | 1.0897 | 16% |
| Pen-Digits (static real) | 1.0218 | 1.0228 | 1.0209 | -4% |
| Shuttle (static real) | 1.1531 | 1.1530 | 1.1477 | -4% |
