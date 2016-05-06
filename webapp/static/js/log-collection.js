$(function(){
    var storeFilters = function(){
        var fieldNames = [],
            storeKey = [$("table").data('collectionName'), 'filters'].join('_');
        $(".active-fields button:not(.active)").each(function(){
            fieldNames.push($(this).data('fieldName'));
        });
        localStorage.setItem(storeKey, JSON.stringify(fieldNames));
    }
    var getFilters = function(){
        var storeKey = [$("table").data('collectionName'), 'filters'].join('_'),
            fieldNames = localStorage.getItem(storeKey);
        if (fieldNames === null){
            return;
        }
        fieldNames = JSON.parse(fieldNames);
        $(".active-fields button").each(function(){
            var $btn = $(this);
            if (fieldNames.indexOf($btn.data('fieldName')) != -1){
                $btn.removeClass('active')
                    .data('fieldHeader').trigger('setHidden', [true]);
            } else {
                $btn.addClass('active')
                    .data('fieldHeader').trigger('setHidden', [false]);
            }
        });
    };
    var getUrlQuery = function(url){
        var query = {};
        if (typeof(url) == 'undefined'){
            url = window.location.href;
        }
        url = url.replace('#', '');
        if (url.split('?').length > 1){
            $.each(url.split('?')[1].split('&'), function(i, qstr){
                var key = qstr.split('=')[0],
                    val = qstr.split('=')[1];
                if (!val.length){
                    return;
                }
                query[key] = decodeURIComponent(val);
            });
            url = url.split('?')[0];
        }
        return {'url':url, 'query':query};
    };
    var buildQueryStr = function(data){
        var l = [];
        $.each(data, function(key, val){
            key = encodeURIComponent(key);
            val = encodeURIComponent(val);
            l.push([key, val].join('='));
        });
        return l.join('&');
    };
    var setLocation = function(data){
        var current = getUrlQuery(),
            currentQuery = current.query;
        if (typeof(data) == 'string'){
            data = {'url':data};
        } else if ($.isPlainObject(data) && typeof(data.url) == 'undefined'){
            data = {'query':data};
        }
        if (typeof(data.query) == 'undefined'){
            data.query = {};
        }
        if (typeof(data.url) == 'undefined'){
            data.url = current.url;
        }
        $.each(data.query, function(key,val){
            currentQuery[key] = val;
        });
        url = [data.url, buildQueryStr(currentQuery)].join('?');
        window.location = url;
    };

    $(".log-entry-field[data-field-name=datetime]").each(function(){
        var $td = $(this),
            d = new Date($td.text());
        $td.text(d.toLocaleString()).data('fieldValue', d);
    });
    $(".field-header-link").click(function(e){
        var $this = $(this),
            $i = $("i", $this),
            fieldName = $this.parent().data('fieldName'),
            query = {};
        e.preventDefault();
        if ($i.hasClass('fa-sort-up')){
            query.s = '-' + fieldName;
        } else if ($i.hasClass('fa-sort-down')){
            query.s = '';
        } else {
            query.s = fieldName;
        }
        setLocation(query);
    });

    $(".active-fields").data('buttons', {});

    $(".active-fields button").each(function(){
        var $btn = $(this);
        $(".active-fields").data('buttons')[$btn.data('fieldName')] = $btn;
    }).click(function(){
        var $btn = $(this),
            $header = $btn.data('fieldHeader'),
            hidden = $btn.hasClass('active')
        $header.trigger('setHidden', [hidden]);
        $btn.toggleClass('active');
        storeFilters();
    });

    $(".log-entry-field").each(function(){
        var $this = $(this),
            $header = $("#F_header".replace('F', $this.data('fieldName'))),
            $filterBtn = $(".active-fields").data('buttons')[$this.data('fieldName')];
        $header
            .data('entryField', $this)
            .on('setHidden', function(e, hidden){
                if (hidden){
                    $this.addClass('hidden');
                } else {
                    $this.removeClass('hidden');
                }
            });
        $filterBtn.data('entryField', $this);
    });
    $(".field-header").each(function(){
        var $this = $(this),
            $filterBtn = $(".active-fields").data('buttons')[$this.data('fieldName')];
        $filterBtn.data('fieldHeader', $this);
        if ($this.hasClass('hidden')){
            $filterBtn.addClass('active');
        }
    }).on('setHidden', function(e, hidden){
        if (hidden){
            $(this).addClass('hidden');
        } else {
            $(this).removeClass('hidden');
        }
    });

    getFilters();


    $.contextMenu({
        selector: ".log-entry-field",
        items:{
            'filter':{
                name: 'Filter by this value',
                callback: function(key, opt){
                    var $td = opt.$trigger;
                    setLocation({
                        p:0,
                        filter_field:$td.data('fieldName'),
                        filter_value:$td.data('fieldValue'),
                    });
                },
            },
            'clearFilter':{
                name: 'Clear filters',
                callback: function(key, opt){
                    console.log(getUrlQuery());
                    if (typeof(getUrlQuery().query.filter_field) == 'undefined'){
                        return;
                    }
                    setLocation({
                        p:0,
                        filter_field:'',
                        filter_value:'',
                    });
                },
            },
        },
    });
});
