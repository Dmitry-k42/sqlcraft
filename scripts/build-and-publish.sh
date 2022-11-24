#!/bin/bash

python3 -m build && twine check dist/* && twine upload dist/*
