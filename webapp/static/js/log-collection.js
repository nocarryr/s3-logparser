$(function(){
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
});
