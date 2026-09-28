| Setting | vs | pairs | hybrid better | median gain | one-sided Wilcoxon p | Holm p |
|---|---|---|---|---|---|---|
| static real | norm | 120 | 31% | -0.0037 | 1 | 1 |
| static real | objective | 120 | 38% | -0.0038 | 1 | 1 |
| drifting (SynDrift) | norm | 30 | 77% | +0.0123 | 1.5e-05 | 0.00012 |
| drifting (SynDrift) | objective | 30 | 47% | -0.0000 | 0.27 | 1 |
| evolving real | norm | 100 | 49% | -0.0003 | 0.32 | 1 |
| evolving real | objective | 100 | 36% | -0.0111 | 0.98 | 1 |
| all | norm | 250 | 44% | -0.0017 | 0.68 | 1 |
| all | objective | 250 | 38% | -0.0035 | 1 | 1 |

Friedman over 250 paired blocks (dataset x setting x budget x seed): mean ranks hybrid 2.18, objective 1.84, top-k magnitude 1.98; chi2 = 14.3, p = 0.00077
