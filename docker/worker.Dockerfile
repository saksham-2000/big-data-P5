FROM p5-base
CMD ["bash", "-c", "/spark-3.5.7-bin-hadoop3/sbin/start-worker.sh spark://boss:7077 && sleep infinity"]

