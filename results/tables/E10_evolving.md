| Comparison | Data | pairs | objective better | median cost-ratio gain | one-sided Wilcoxon p |
|---|---|---|---|---|---|
| objective vs norm | NSL-KDD | 25 | 60% | +0.0259 | 0.048 |
| objective vs norm | Shuttle | 25 | 56% | +0.0207 | 0.33 |
| objective vs norm | Pen-Digits | 25 | 32% | -0.0051 | 0.88 |
| objective vs norm | Letter | 25 | 80% | +0.0079 | 0.0057 |
| objective vs norm | all 4 real evolving | 100 | 57% | +0.0050 | 0.093 |
| objective vs uniform | NSL-KDD | 25 | 40% | -0.0289 | 0.77 |
| objective vs uniform | Shuttle | 25 | 60% | +0.0580 | 0.038 |
| objective vs uniform | Pen-Digits | 25 | 44% | -0.0034 | 0.71 |
| objective vs uniform | Letter | 25 | 64% | +0.0057 | 0.045 |
| objective vs uniform | all 4 real evolving | 100 | 52% | +0.0013 | 0.29 |

kB needed to reach cost ratio <= 1.05 / <= 1.10 on evolving streams

| Data | objective | top-k magnitude | model-free | + adaptive res. | k-FED | Periodic-Δ |
|---|---|---|---|---|---|---|
| NSL-KDD | 232 / 174 | — / 204 | — / 157 | — / — | — / — | — / — |
| Shuttle | 38 / 35 | 35 / 32 | 100 / 40 | 42 / 33 | — / — | — / — |
| Pen-Digits | 116 / 72 | 102 / 69 | 110 / 74 | 52 / 33 | — / 58 | — / 194 |
| Letter | 154 / 62 | 161 / 70 | 162 / 66 | 154 / 62 | — / 150 | — / 244 |
