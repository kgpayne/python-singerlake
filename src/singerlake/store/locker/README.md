## Example Approach

Let the service in question who needs the lock be called S1.

1. The S1 has a UUID as a static in-memory variable - S1-UUID.

2. First read the file s3://bucket/locks/lock.json with JSON format: {UUID:'1-2-3-4' , createdTime:yyyy-mm-dd-hh-mm-ss-ms};

3. Does the UUID from JSON match with S1-UUID? yes? then you already have the lock. return true. If the string doesn't match...

4. Was the lock created more than abort time? yes? Then move to step 5. No? then return saying you don't have the lock. return false.

5. Write S1-UUID and the current time to JSON and write it to s3://bucket/lock/lock.json

6. Wait for 250ms --250 because I have never seen S3 latency to be more than 125ms; so taking double time to play safe. Read the lock.json again. Read the UUID from the JSON if the UUID matches S1-UUID then you get the lock. return true. If not return false.

For HTTP API calls I put the abort time as 2sec (because HTTP SLA is 2sec to our APIs). For spark jobs with higher SLA, we put 2hrs --because the spark jobs take 2hrs to complete.

## Example Implementations

- https://github.com/jfstephe/aws-s3-lock
