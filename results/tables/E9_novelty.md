| Comparison | Data | pairs | objective better | median cost-ratio gain | one-sided Wilcoxon p |
|---|---|---|---|---|---|
| objective vs norm | drifting (SynDrift) | 30 | 80% | +0.0122 | 1.5e-05 |
| objective vs norm | static (4 real datasets) | 120 | 45% | -0.0007 | 0.71 |
| objective vs norm | all | 150 | 52% | +0.0001 | 0.072 |
| objective vs uniform | drifting (SynDrift) | 30 | 73% | +0.0091 | 0.00012 |
| objective vs uniform | static (4 real datasets) | 120 | 52% | +0.0004 | 0.023 |
| objective vs uniform | all | 150 | 56% | +0.0014 | 0.00053 |

kB needed to reach cost ratio <= 1.05 (and <= 1.10)

| Data | objective | + adaptive q≥k | + adaptive q≥2k | top-k magnitude | model-free | k-FED | Periodic-Δ |
|---|---|---|---|---|---|---|---|
| SynDrift | 43 / 40 | 32 / 22 | 35 / 28 | 55 / 46 | 57 / 49 | 113 / 60 | 256 / 154 |
| NSL-KDD | 362 / 122 | 467 / 363 | 467 / 363 | 389 / 133 | — / 113 | — / 45 | 484 / 206 |
| Shuttle | 133 / 86 | 124 / 84 | 124 / 84 | 107 / 76 | — / 356 | — / 62 | — / 189 |
| Pen-Digits | 45 / 43 | 77 / 48 | 25 / 21 | 44 / 42 | 43 / 43 | 53 / 27 | 140 / 100 |
| Letter | 83 / 43 | 296 / 92 | 83 / 43 | 78 / 41 | 76 / 43 | 204 / 76 | 256 / 137 |
