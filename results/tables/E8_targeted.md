| Component | Setting | Variant | kB up | wire kB | cost ratio | target metric |
|---|---|---|---|---|---|---|
| link price | NSL-KDD, B=150 | price=False|B=150 | 516.8 | 524.6 | 1.033 | bytes(bad links)/bytes(good links) 1.02 |
| link price | NSL-KDD, B=500 | price=False|B=500 | 1414.2 | 1445.9 | 1.016 | bytes(bad links)/bytes(good links) 1.04 |
| link price | NSL-KDD, B=150 | price=True|B=150 | 513.6 | 522.4 | 1.034 | bytes(bad links)/bytes(good links) 1.01 |
| link price | NSL-KDD, B=500 | price=True|B=500 | 1370.7 | 1397.3 | 1.018 | bytes(bad links)/bytes(good links) 0.99 |
| dual ascent | Pen-Digits, B=500 | dual=False | 444.1 | 449.3 | 0.999 | budget used 30% |
| dual ascent | Pen-Digits, B=500 | dual=True | 928.3 | 943.5 | 0.999 | budget used 62% |
| dual ascent | SynDrift, B=500 | dual=False | 533.6 | 540.1 | 1.007 | budget used 36% |
| dual ascent | SynDrift, B=500 | dual=True | 1268.7 | 1290.8 | 1.004 | budget used 85% |
| novelty override | SynDrift, B=10 | novelty=False | 58.6 | 60.4 | 1.060 | detection delay 10.9 ± 5.6 s |
| novelty override | SynDrift, B=20 | novelty=False | 86.8 | 88.5 | 1.035 | detection delay 9.4 ± 2.6 s |
| novelty override | SynDrift, B=10 | novelty=True | 58.8 | 60.6 | 1.051 | detection delay 10.9 ± 5.6 s |
| novelty override | SynDrift, B=20 | novelty=True | 87.3 | 88.8 | 1.035 | detection delay 9.4 ± 2.6 s |
| link price | SynDrift, B=150 | price=False|B=150 | 460.0 | 469.9 | 1.005 | bytes(bad links)/bytes(good links) 1.00 |
| link price | SynDrift, B=500 | price=False|B=500 | 1320.5 | 1350.5 | 1.004 | bytes(bad links)/bytes(good links) 0.97 |
| link price | SynDrift, B=150 | price=True|B=150 | 450.6 | 460.0 | 1.006 | bytes(bad links)/bytes(good links) 0.97 |
| link price | SynDrift, B=500 | price=True|B=500 | 1276.2 | 1305.0 | 1.004 | bytes(bad links)/bytes(good links) 0.93 |
