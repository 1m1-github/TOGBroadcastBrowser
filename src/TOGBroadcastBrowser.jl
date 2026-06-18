module TOGBroadcastBrowser

using HTTP, URIs, Sockets
using LoopOS: BatchProcessor, start!, Peripheral
import Base.put!

"Serve and execute JavaScript on an HTTP client using SSE"
struct BroadcastBrowser <: Peripheral
    stream::HTTP.Stream
    width::Int
    height::Int
    processor::BatchProcessor{String}
    BroadcastBrowser(stream, width, height) = new(stream, width, height, BatchProcessor{String}())
end
const CLIENTS = Ref(Set{BroadcastBrowser}())
"`put!(BroadcastBrowser, js)` runs the js on all connected browsers"
put!(::Type{BroadcastBrowser}, js) = [put!(client.processor, js) for client = CLIENTS[]]

const HTML = raw"""
<!DOCTYPE html>
<html>
<body>
<script>
const sse = new EventSource(`/events?width=${document.documentElement.clientWidth}&height=${document.documentElement.clientHeight}`)
sse.onmessage = (e) => eval(e.data)
document.addEventListener('keydown', (e) => {
  fetch('/keypress', {
    method: 'POST',
    body: e.key
})})
</script>
</body>
</html>
"""

function safe_write(stream, js)
    try
        write(stream, js)
        flush(stream)
        true
    catch e
        e isa Base.IOError || rethrow()
        false
    end
end

function handle_sse(a)
    HTTP.setstatus(a.stream, 200)
    HTTP.setheader(a.stream, "Content-Type" => "text/event-stream")
    HTTP.setheader(a.stream, "Cache-Control" => "no-cache")
    HTTP.startwrite(a.stream)
    start!(a.processor) do input
        for js = input
            js = replace(js, "\n" => ";")
            safe_write(a.stream, "data: $js\n\n") || return
        end
    end
end

function openport(hint)
    port, server = listenany(hint)
    close(server)
    Int(port)
end

function awaken(;root::Function, keypress::Function, port=openport(8888))
    @async HTTP.serve("0.0.0.0", port; stream=true) do stream
        target = stream.message.target
        if target == "/"
            HTTP.setstatus(stream, 200)
            HTTP.setheader(stream, "Content-Type" => "text/html")
            HTTP.startwrite(stream)
            write(stream, HTML)
        elseif (uri = URI(target); uri.path == "/events")
            params = queryparams(uri)
            width = parse(Int, params["width"])
            height = parse(Int, params["height"])
            bb = BroadcastBrowser(stream, width, height)
            push!(CLIENTS[], bb)
            root(port, bb)
            handle_sse(bb)
            delete!(CLIENTS[], bb)
        elseif uri.path == "/keypress"
            keypress(String(read(stream)))
            HTTP.setstatus(stream, 204)
            HTTP.startwrite(stream)
        else
            HTTP.setstatus(stream, 404)
            HTTP.startwrite(stream)
        end
    end
end

end
