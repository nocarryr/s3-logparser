$(function(){
    var storeFilters = function(){
        var fieldNames = [],
            storeKey = [$("table").data('collectionName'), 'filters'].join('_');
        $(".active-fields button.active").each(function(){
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
        $.each(fieldNames, function(i, fieldName){
            var $btn = $(".active-fields").data('buttons')[fieldName];
            if ($btn.hasClass('active')){
                return;
            }
            $btn
                .addClass('active')
                .data('fieldHeader').trigger('setHidden', [true]);
        });
    };

    $(".log-entry-field[data-field-name=datetime]").each(function(){
        var $td = $(this),
            d = new Date($td.text());
        $td.text(d.toLocaleString()).data('fieldValue', d);
    });
    $(".field-header-link").click(function(e){
        var $this = $(this),
            url = window.location.href,
            $i = $("i", $this),
            fieldName = $this.parent().data('fieldName'),
            query = {};
        url = url.replace('#', '');
        if (url.split('?').length > 1){
            $.each(url.split('?')[1].split('&'), function(i, qstr){
                query[qstr.split('=')[0]] = qstr.split('=')[1];
            });
            url = url.split('?')[0];
        }
        e.preventDefault();
        if ($i.hasClass('fa-sort-up')){
            query.s = '-' + fieldName;
        } else if ($i.hasClass('fa-sort-down')){
            query.s = '';
        } else {
            query.s = fieldName;
        }
        url = [url, $.param(query)].join('?');
        window.location = url;
    });

    $(".active-fields").data('buttons', {});

    $(".active-fields button").each(function(){
        var $btn = $(this);
        $(".active-fields").data('buttons')[$btn.data('fieldName')] = $btn;
    }).click(function(){
        var $btn = $(this),
            $header = $btn.data('fieldHeader'),
            hidden = !$btn.hasClass('active')
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

});
