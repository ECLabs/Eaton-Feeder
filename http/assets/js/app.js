$(function(){
    if(!window["WebSocket"]){
        alert("Your browser does not support WebSockets.  You will not be able to use this application.");
        return
    }
    
    function Log(options){
        var me = this;
        this.showInfo = options.showInfo;
        this.showDebug = options.showDebug;
        this.showError = options.showError;
        this.showTrace = options.showTrace;
        
        this.Message = ko.observable(options.Message);
        this.Level = ko.observable(options.Level);
        this.Date = ko.observable(options.Date);
        
        this.isVisible = ko.computed(function(){
            if(me.Level() == "info" && me.showInfo()){
                return true;
            }
            if(me.Level() == "debug" && me.showDebug()){
                return true;
            }
            if(me.Level() == "error" && me.showError()){
                return true;
            }
            if(me.Level() == "trace" && me.showTrace()){
                return true;
            }
            return false;
        });
    };
    
    function Application(){
        var me = this;
        this.conn = new WebSocket("ws://"+window.location.host+"/ws");
        this.count = ko.observable(0);
        this.logs = ko.observableArray([]);
        this.showInfo = ko.observable(true);
        this.showDebug = ko.observable(false);
        this.showError = ko.observable(true);
        this.showTrace = ko.observable(false);
        this.forceScroll = ko.observable(true);
        this.conn.onclose = function(evt) {
            console.log("closed websocket!");
        };
        this.conn.onmessage = function(evt) {
            var json = JSON.parse(evt.data)
            json.showInfo = me.showInfo;
            json.showDebug = me.showDebug;
            json.showError = me.showError;
            json.showTrace = me.showTrace;
            me.logs.push(new Log(json));
            me.count(me.count()+1);
            if(me.logs().length > 1000){
                me.logs.shift();
            }
            $('html body').animate({ 
               scrollTop: $(document).height()-$(window).height()}, 
               1, 
               "linear"
            );
        };
        this.conn.onopen = function(){
            console.log("opened connection!")
        };
    };
    
    ko.applyBindings(new Application())
})