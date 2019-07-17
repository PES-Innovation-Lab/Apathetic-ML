#touch standarda standardb
python3 worker.py
#gunicorn -b 0.0.0.0:4000 worker:app --timeout 360 --log-level=debug
