function sendAjax() {
    var startLatitude = $("#startLatitude").val();
    var startLongitude = $("#startLongitude").val();
    var k = $("#k").val();
    var distance = $("#distance").val();
    var selectType = $("#selectType").val();
    //alert("start：　" + startLatitude + " " + startLongitude
    //    +" k: " + k + " distance: " + distance + " selectType: " + selectType);
    $("#submitBtn").attr("disabled", true);
    $("#submitBtn").html("查询中，请稍候...") ;
    $.ajax({
        url: 'http://localhost:8080/MavenDemo/controll/KNNQuery',
        type: 'post',
        dataType: 'json',
        data: {
            latitude: startLatitude,
            longitude: startLongitude,
            k: k,
            distance: distance,
            type: selectType
        },
        success: function(data) {
            $("#resultArea").html("查询结果： " + data.result + "\n");
            $("#resultArea").append("用时： " + data.time + "\n");
            $("#resultArea").append("满足条件的" + data.points.length + "个坐标(经度,纬度)为： ");
            for (var i = 0; i < data.points.length; i ++) {
                if (i % 3 == 0) {
                    $("#resultArea").append("\n");
                }
                $("#resultArea").append(" (" + data.points[i].longitude + "," + data.points[i].latitude + ")");
            }
            $("#submitBtn").html("开始查询") ;
            $("#submitBtn").attr("disabled", false);
        }

    });
}