# Notes

This repository aim at parsing [Wikipedia comparable corpora](https://linguatools.org/tools/corpora/wikipedia-comparable-corpora/) from linguatools and building bilingual co-occurence matrix for further analysis.

At this time, we only build en-zh and en-ja bilingual co-occurence matrix.

The execution steps are
- download corpora
- parse corpora by `Wiki_processing.ipynb`
- apply NLP by `*_NLP.ipynb(.py)`
- create co-occurence matrix by `*_NV_pairString.ipynb`