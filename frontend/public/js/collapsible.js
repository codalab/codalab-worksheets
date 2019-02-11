$(document.body).on('click', '.collapsible-header' ,function(){
    $header = $(this);
    $content = $header.next();
    $content.slideToggle(150, function () {
        $header.html(function () {
            return $content.is(":visible") ? ($header.html()).replace(/\u25B8/, '\u25BE') : ($header.html()).replace(/\u25BE/, '\u25B8');
        });
    });
});
