
var socket = io();
socket.on("logs", function(level, message){
    console.log("Msg: ", arguments);
});
setInterval(function(){
    socket.emit("test", "woo")
}, 1000)