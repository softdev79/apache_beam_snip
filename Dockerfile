FROM openjdk:11-jre-slim
COPY build/libs/my-application-0.1.0.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
