#! /bin/sh
set -e

exec ./pd-server \
     --name "pd" \
     --client-urls http://0.0.0.0:2379 \
     --peer-urls http://0.0.0.0:2380 \
     --advertise-client-urls http://`cat /etc/hostname`:2379 \
     --advertise-peer-urls http://`cat /etc/hostname`:2380 \
