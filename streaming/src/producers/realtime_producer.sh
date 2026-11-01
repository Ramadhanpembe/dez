#!/bin/zsh

# The URL is not found!
PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"
curl -fsSL ${PREFIX}/src/producers/producer_realtime.py -P src/producers/

