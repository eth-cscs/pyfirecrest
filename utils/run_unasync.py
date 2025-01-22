import unasync


unasync.unasync_files(
    ['firecrest/v2/_async/Client.py'],
    rules=[
        unasync.Rule(
            fromdir="firecrest/v2/_async/",
            todir="firecrest/v2/_sync/",
            additional_replacements={
                "AsyncFirecrest": "Firecrest",
                "AsyncClient": "Client",
                "aclose": "close",
                # "asyncio.sleep": "time.sleep",
                # multi token replacement doesn't work, it happens manually
                # TODO find a way to replace this automatically
            }
        ),
    ]
)
