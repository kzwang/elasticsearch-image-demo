
function searchURL(feature) {
    var url = $("input[name='url']").val();
    window.location.href = "/?url=" + url + "&feature=" +feature;
}