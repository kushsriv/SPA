kB needed to reach cost ratio <= 1.05 / <= 1.10 (all methods use 8-bit quantised messages)

| Setting | FedCAST-v2 (exact value + 8-bit) | top-k magnitude + 8-bit | Periodic-Δ + 8-bit | k-FED + 8-bit | best baseline / FedCAST (1.05) |
|---|---|---|---|---|---|
| SynDrift (drifting) | 18 / 18 | 28 / 25 | — / 69 | — / 29 | 1.53x |
| NSL-KDD (static) | 84 / 40 | 85 / 45 | 121 / 59 | — / 21 | 1.01x |
| Shuttle (static) | 69 / 45 | 54 / 41 | — / — | — / — | 0.77x |
| Pen-Digits (static) | 21 / 21 | 18 / 18 | 56 / 40 | 26 / 13 | 0.89x |
| Letter (static) | 54 / 21 | 34 / 19 | 104 / 55 | — / 32 | 0.63x |
| NSL-KDD (evolving) | 133 / 90 | 147 / 80 | — / — | — / — | 1.11x |
| Shuttle (evolving) | 27 / 22 | 21 / 19 | — / — | — / — | 0.78x |
| Pen-Digits (evolving) | 29 / 21 | 52 / 32 | — / 78 | — / 29 | 1.82x |
| Letter (evolving) | 106 / 26 | 72 / 30 | — / 98 | — / 63 | 0.68x |
| Gas-Drift (natural) | — / — | — / — | — / — | — / — | FedCAST does not reach 1.05 |
| CoverType (natural) | — / 121 | — / 159 | — / — | — / — | FedCAST does not reach 1.05 |
| Intel-Lab (natural) | 66 / 37 | 83 / 57 | — / — | — / — | 1.25x |

Best cost ratio attainable within a budget of 1% / 3% / 10% of Centralised-Raw bytes

| Setting | Budget | FedCAST-v2 (exact value + 8-bit) | top-k magnitude + 8-bit | Periodic-Δ + 8-bit | k-FED + 8-bit |
|---|---|---|---|---|---|
| SynDrift (drifting) | 1% | **1.016** | 1.052 | 1.161 | 1.127 |
| SynDrift (drifting) | 3% | **1.009** | 1.013 | 1.076 | 1.078 |
| SynDrift (drifting) | 10% | 1.009 | **1.007** | 1.076 | 1.078 |
| NSL-KDD (static) | 1% | **1.043** | 1.048 | 1.049 | 1.081 |
| NSL-KDD (static) | 3% | 1.043 | **1.028** | 1.038 | 1.081 |
| NSL-KDD (static) | 10% | 1.043 | **1.028** | 1.038 | 1.081 |
| Shuttle (static) | 1% | 1.212 | 1.247 | 1.318 | **1.172** |
| Shuttle (static) | 3% | 1.032 | **1.004** | 1.116 | 1.172 |
| Shuttle (static) | 10% | 1.032 | **1.004** | 1.116 | 1.172 |
| Pen-Digits (static) | 1% | — | — | — | **1.137** |
| Pen-Digits (static) | 3% | 1.027 | **1.021** | 1.268 | 1.067 |
| Pen-Digits (static) | 10% | 1.007 | **1.004** | 1.012 | 1.035 |
| Letter (static) | 1% | — | — | — | **1.305** |
| Letter (static) | 3% | 1.054 | **1.044** | 1.160 | 1.091 |
| Letter (static) | 10% | 1.042 | **1.029** | 1.043 | 1.052 |
| NSL-KDD (evolving) | 1% | **1.049** | 1.091 | 1.319 | 1.214 |
| NSL-KDD (evolving) | 3% | 1.049 | **1.044** | 1.177 | 1.214 |
| NSL-KDD (evolving) | 10% | 1.049 | **1.044** | 1.177 | 1.214 |
| Shuttle (evolving) | 1% | 1.072 | **1.013** | 1.640 | 1.371 |
| Shuttle (evolving) | 3% | **0.879** | 0.900 | 1.331 | 1.371 |
| Shuttle (evolving) | 10% | 0.879 | **0.861** | 1.331 | 1.371 |
| Pen-Digits (evolving) | 1% | — | — | — | **1.459** |
| Pen-Digits (evolving) | 3% | **1.046** | 1.114 | 2.192 | 1.189 |
| Pen-Digits (evolving) | 10% | **1.017** | 1.024 | 1.054 | 1.066 |
| Letter (evolving) | 1% | — | — | 1.799 | **1.407** |
| Letter (evolving) | 3% | **1.070** | 1.075 | 1.403 | 1.187 |
| Letter (evolving) | 10% | 1.048 | **1.030** | 1.070 | 1.082 |
| Gas-Drift (natural) | 1% | — | 1.663 | — | **1.281** |
| Gas-Drift (natural) | 3% | **1.136** | 1.210 | 1.268 | 1.166 |
| Gas-Drift (natural) | 10% | 1.136 | 1.210 | **1.125** | 1.166 |
| CoverType (natural) | 1% | **1.083** | 1.244 | 1.313 | 1.149 |
| CoverType (natural) | 3% | 1.083 | **1.068** | 1.148 | 1.149 |
| CoverType (natural) | 10% | 1.083 | **1.068** | 1.148 | 1.149 |
| Intel-Lab (natural) | 1% | **1.192** | 1.429 | 2.194 | 3.284 |
| Intel-Lab (natural) | 3% | 1.092 | **1.062** | 1.211 | 3.284 |
| Intel-Lab (natural) | 10% | 1.036 | **1.017** | 1.211 | 3.284 |

Friedman over 31 blocks where all four are feasible: mean ranks FedCAST-v2 (exact value + 8-bit) 1.71, top-k magnitude + 8-bit 1.58, Periodic-Δ + 8-bit 3.19, k-FED + 8-bit 3.52; chi2 = 55.5, p = 5.4e-12, Nemenyi CD = 0.84

| Setting group | pairs | fedcast-v2 better than top-k+8-bit | mean gain | one-sided Wilcoxon p |
|---|---|---|---|---|
| static | 100 | 29% | -0.0065 | 1 |
| drifting | 25 | 60% | +0.0605 | 0.0014 |
| evolving | 100 | 61% | +0.0149 | 0.0053 |
| natural | 75 | 69% | +0.0626 | 4.1e-05 |
| non-stationary | 200 | 64% | +0.0385 | 1.6e-07 |
| all | 300 | 52% | +0.0235 | 0.00017 |
