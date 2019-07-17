#touch standarda standardb
python3 subworker.py
#gunicorn -b 0.0.0.0:4000 subworker:app --timeout 360 --log-level=debug
