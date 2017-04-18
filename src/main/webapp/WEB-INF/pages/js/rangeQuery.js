function sendAjax() {
    var startLatitude = $("#startLatitude").val();
    var startLongitude = $("#startLongitude").val();
    var endLatitude = $("#endLatitude").val();
    var endLongitude = $("#endLongitude").val();
    var selectType = $("#selectType").val();
    //alert("start：　" + startLatitude + " " + startLongitude
    //    +" end: " + endLatitude + " " + endLongitude + " type: " + selectType);
    $("#submitBtn").attr("disabled", true);
    $("#submitBtn").html("查询中，请稍候...") ;
    $.ajax({
        url: 'http://localhost:8080/MavenDemo/controll/RangeQuery',
        type: 'post',
        dataType: 'json',
        data: {
            startLatitude: startLatitude,
            startLongitude: startLongitude,
            endLatitude: endLatitude,
            endLongitude: endLongitude,
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
            $("#submitBtn").attr("disabled", false);
            $("#submitBtn").html("开始查询") ;
        }

    });
}
