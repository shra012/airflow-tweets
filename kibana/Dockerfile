# Dockerfile
FROM docker.elastic.co/logstash/logstash:8.8.0

# 1. Switch to root to copy files & set perms
USER root
# 2. Copy in your drivers, pipeline, config & keys directories
COPY logstash/drivers/   /usr/share/logstash/drivers/
COPY logstash/pipeline/  /usr/share/logstash/pipeline/
COPY logstash/config/    /usr/share/logstash/config/
COPY logstash/keys/      /usr/share/logstash/keys/

# 3. Fix ownership & permissions in one go
RUN chown -R logstash:logstash \
      /usr/share/logstash/drivers \
      /usr/share/logstash/pipeline \
      /usr/share/logstash/config \
      /usr/share/logstash/keys \
 && chmod -R 644 /usr/share/logstash/drivers/*.jar \
 && chmod -R 600 /usr/share/logstash/keys/*.json

# 4. Back to the logstash user and default entrypoint
USER logstash

ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]
CMD ["logstash", "-f", "/usr/share/logstash/pipeline/logstash.conf"]