# `POST /api/worksheets/command/`
# `GET /api/worksheets/<uuid:re:0x[0-9a-f]{32}>/`
# `POST /api/worksheets/<uuid:re:0x[0-9a-f]{32}>/`
# `GET /api/bundles/content/<uuid:re:0x[0-9a-f]{32}>/<path:path>/`
# `GET /api/bundles/content/<uuid:re:0x[0-9a-f]{32}>/`
# `GET /api/bundles/<uuid:re:0x[0-9a-f]{32}>/`
# `POST /api/bundles/<uuid:re:0x[0-9a-f]{32}>/`
    
Save metadata information for a bundle.

# `GET /api/chatbox/`
    
Return a list of chats that the current user has had

# `POST /api/chatbox`
    
Add the chat to the log.
Return an auto response, if the chat is directed to the system.
Otherwise, return an updated chat list of the sender.

# `GET /api/users/`
# `GET /api/faq/`
    
Return a list of Frequently Asked Questions.
Currently disabled. Needs further work.

# `POST /api/rpc`
    
Temporary interface for making simple RPC calls to BundleService methods over
the REST API, to speed up deprecation of XMLRPC while we migrate to REST.

RPC calls should be POST requests with a JSON payload:
{
    &#039;method&#039;: &lt;name of the BundleService method to call&gt;,
    &#039;args&#039;: &lt;array of args&gt;,
    &#039;kwargs&#039;: &lt;object of kwargs&gt;
}

