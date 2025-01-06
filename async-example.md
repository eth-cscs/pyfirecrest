# Examples for asyncio with pyfirecrest

### Simple asynchronous workflow with the new client

Here is an example of how to use the `AsyncFirecrest` client with asyncio.

```python
import firecrest
import asyncio
import logging


# Setup variables before running the script
client_id = ""
client_secret = ""
token_uri = ""
firecrest_url = ""

machine = ""
local_script_path = ""

# Ignore this part, it is simply setup for logging
logger = logging.getLogger("simple_example")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt="%H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

async def workflow(client, i):
    logger.info(f"{i}: Starting workflow")
    job = await client.submit(machine, local_script_path)
    logger.info(f"{i}: Submitted job with jobid: {job['jobid']}")
    while True:
        poll_res = await client.poll_active(machine, [job["jobid"]])
        if len(poll_res) < 1:
            logger.info(f"{i}: Job {job['jobid']} is no longer active")
            break

        logger.info(f"{i}: Job {job['jobid']} status: {poll_res[0]['state']}")
        await asyncio.sleep(30)

    output = await client.view(machine, job["job_file_out"])
    logger.info(f"{i}: job output: {output}")


async def main():
    auth = firecrest.ClientCredentialsAuth(client_id, client_secret, token_uri)
    client = firecrest.v1.AsyncFirecrest(firecrest_url, authorization=auth)

    # Set up the desired polling rate for each microservice. The float number
    # represents the number of seconds between consecutive requests in each
    # microservice.
    client.time_between_calls = {
        "compute": 1,
        "reservations": 0.5,
        "status": 0.5,
        "storage": 0.5,
        "tasks": 0.5,
        "utilities": 0.5,
    }

    workflows = [workflow(client, i) for i in range(5)]
    await asyncio.gather(*workflows)


asyncio.run(main())

```


### External transfers with `AsyncFirecrest`

The uploads and downloads work as before but you have to keep in mind which methods are coroutines.

```python
# Download
down_obj = await client.external_download("cluster", "/remote/path/to/the/file")
status = await down_obj.status
print(status)
await down_obj.finish_download("my_local_file")

# Upload
up_obj = await client.external_upload("cluster", "/path/to/local/file", "/remote/path/to/filesystem")
await up_obj.finish_upload()
status = await up_obj.status
print(status)
```
