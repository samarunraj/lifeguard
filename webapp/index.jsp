<%@ page %>

<html>
<body>
<head>
<title>Lifeguard</title>
    <style type="text/css">
        @import "js/dijit/themes/tundra/tundra.css";
        @import "js/dojo/resources/dojo.css"
    </style>
    <script type="text/javascript" src="js/dojo/dojo.js.uncompressed.js"
            djConfig="parseOnLoad: true, isDebug: true"></script>
	<script language="javascript"> <!--
		dojo.require("dojo.parser");
		dojo.require("dijit.layout.ContentPane");
		dojo.require("dijit.layout.LayoutContainer");
		dojo.require("dijit.layout.TabContainer");
		dojo.require("dijit.form.Button");
		dojo.require("lg_widgets.PoolStatus");

		function wireTabs() {
			tabSet = dijit.byId("mainTabContainer");
			tabs = tabSet.getChildren();
			for (i=0; i<tabs.length; i++ ) {
				tabs[i].setHref("PoolStatus.jsp?serviceName="+tabs[i].id);
			}
			tabSet.selectChild(tabs[0]);
		}

		function selectTool(tool_url, tool_cb) {
			div = dijit.byId("toolHolder");
			dojo.xhrGet( {
				url: tool_url,
				handleAs: "text",
				load: function(responseObject, ioArgs) {
					div.setContent(responseObject);
					div.startup();
					if (tool_cb) {
						div.onLoad(tool_cb());
					}
					return responseObject;
				}
			});
		}
	--> </script>
</head>
<body onLoad="" class="tundra">

<div dojoType="dijit.layout.LayoutContainer" style="width: 100%; height: 100%; padding: 0; margin: 0; border: 0;">
    <div dojoType="dijit.layout.ContentPane" layoutAlign="top" style="background:#eeeeee">
		<img src="images/lifeguard-logo.gif" align="center"/><span style="font-family:Arial; font-size:14pt">Lifeguard</span>
    </div>
    <div dojoType="dijit.layout.ContentPane" layoutAlign="left" style="width: 120px;">
        <button dojoType="dijit.form.Button" onclick="selectTool('ActivePools.jsp', null)">Active Pools</button>
        <button dojoType="dijit.form.Button" onclick="selectTool('JobStatus.jsp', null)">Job Status</button>
    </div>
    <div id="toolHolder" dojoType="dijit.layout.ContentPane" layoutAlign="client">
    </div>
</div>
</body>
</html>

