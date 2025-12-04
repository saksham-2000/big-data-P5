FROM p5-base
CMD ["bash", "-c", "/spark-3.5.7-bin-hadoop3/sbin/start-master.sh && sleep infinity"]
