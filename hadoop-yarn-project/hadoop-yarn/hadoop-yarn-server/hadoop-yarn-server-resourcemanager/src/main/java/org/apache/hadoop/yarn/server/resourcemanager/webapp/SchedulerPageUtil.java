/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class SchedulerPageUtil {

  static class QueueBlockUtil extends HtmlBlock {

    private void reopenQueue(Block html) {
      html.
          script().$type("text/javascript").
          __("function reopenQueryNodes() {",
            "  var currentParam = decodeURIComponent(window.location.href)"
                + ".split('?');",
            "  var tmpCurrentParam = currentParam;",
            "  var queryQueuesString = '';",
            "  if (tmpCurrentParam.length > 1) {",
            "    // openQueues=q1#q2&param1=value1&param2=value2",
            "    tmpCurrentParam = tmpCurrentParam[1];",
            "    if (tmpCurrentParam.indexOf('openQueues=') != -1 ) {",
            "      tmpCurrentParam = tmpCurrentParam.split('openQueues=')[1].split('&')[0];",
            "      queryQueuesString = tmpCurrentParam;",
            "    }",
            "  }",
            "  if (queryQueuesString != '') {",
            "    queueArray = queryQueuesString.split('#');",
            "    $('#cs .q').each(function() {",
            "      var name = $(this).html();",
            "      if (name != 'root' && $.inArray(name, queueArray) != -1) {",
            "        $(this).closest('li').removeClass('jstree-closed').addClass('jstree-open'); ",
            "      }",
            "    });",
            "  }",
            "  $('#cs').bind( {",
            "                  'open_node.jstree' :function(e, data) { storeExpandedQueue(e, data); },",
            "                  'close_node.jstree':function(e, data) { storeExpandedQueue(e, data); }",
            "  });",
            "}").__();
    }

    private void storeExpandedQueue (Block html) {
      html.
          script().$type("text/javascript").
          __("function storeExpandedQueue(e, data) {",
            "  var OPEN_QUEUES = 'openQueues';",
            "  var queueName = e.node.key;", // Get queue name from Wunderbaum event
            "  var action = e.node.expanded ? 'open' : 'close';",
            "  var currentParam = window.location.href.split('?');",
            "  var queueString = '';",
            "  if (currentParam.length > 1) {",
            "    var tmpCurrentParam = currentParam[1].split('&');",
            "    var len = tmpCurrentParam.length;",
            "    var paramExist = false;",
            "    queryString = tmpCurrentParam.map(function(param) {",
            "       if (param.startsWith(OPEN_QUEUES + '=')) {",
            "          paramExist = true;",
            "          return action === 'open' ? addQueueName(param, queueName) : removeQueueName(param, queueName);",
            "       }",
            "       return param;",
            "    }).join('&');",
            "    if (action === 'open' && !paramExist) {",
            "       queryString += '&' + OPEN_QUEUES + '=' + queueName;",
            "    }",
            "  } else if (action === 'open') {",
            "    queryString = OPEN_QUEUES + '=' + queueName;",
            "  }",
            "  queryString = queryString ? '?' + queryString : '';",
            "  var url = window.location.protocol + '//' + window.location.host + window.location.pathname + queryString;",
            "  window.history.pushState({ path: url }, '', url);",
            "}",
            "function removeQueueName(queryString, queueName) {",
            "  queryString = decodeURIComponent(queryString);",
            "  var index = queryString.indexOf(queueName);",
            "  // Finding if queue is present in query param then only remove it",
            "  if (index != -1) {",
            "    var parts = queryString.split('#').filter(q => q !== queueName);",
            "    queryString = parts.length ? OPEN_QUEUES + '=' + parts.join('#') : '';",
            "  }",
            "  return queryString;",
            "}",
            "",
            "function addQueueName(queryString, queueName) {",
            "  var queueArray = queryString.split('#');",
            "  if (!queueArray.includes(queueName)) {",
            "    queryString += '#' + queueName;",
            "  }",
            "  return queryString;",
            "}").__();
    }

    @Override protected void render(Block html) {
      // reopenQueue(html);
      storeExpandedQueue(html);
    }
  }
}
