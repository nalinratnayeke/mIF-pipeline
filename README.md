# MIF Pipeline: InstanSeg → Nimbus

This repo automates:
- Merging multiplex IF channels into two OME-TIFFs (segmentation subset + full panel)
- InstanSeg whole-slide segmentation (conda env `instanseg`)
- Export of full-resolution instance masks for Nimbus
- Nimbus inference in channel chunks (conda env `nimbus`), including prediction images + cell tables

## Install (developer mode)
```bash
pip install -e .