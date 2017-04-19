#!/bin/bash

goodness=$(cat | wc -c)
echo "{\"goodness\": $goodness}"
