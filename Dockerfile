FROM gradle:8.10.2-jdk21 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts gradle.properties ./
COPY app app
RUN gradle clean installDist --no-daemon
# OpenJ9: ~1/3 of the HotSpot footprint on the r2d2 Pi (2026-10-17)
FROM ibm-semeru-runtimes:open-21-jre
WORKDIR /app
COPY --from=build /app/build/install/trama/ /app/
EXPOSE 8080
ENV JAVA_OPTS=""
CMD ["sh", "-c", "bin/trama"]
