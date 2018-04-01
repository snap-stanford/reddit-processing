#!/usr/bin/env bash

ls \
    -alth .. || echo oh no && exit $?
echo still going 
