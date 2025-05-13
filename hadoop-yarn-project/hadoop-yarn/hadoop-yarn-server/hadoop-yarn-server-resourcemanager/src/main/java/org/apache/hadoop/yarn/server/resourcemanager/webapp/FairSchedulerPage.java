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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.Collection;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

public class FairSchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px solid #000000";
  static final String Q_INSTANTANEOUS_FS =
      "left:0%;background:none;border:1px dashed #000000";
  static final String Q_OVER = "background:#FFA333";
  static final String Q_UNDER = "background:#5BD75B";
  static final String STEADY_FAIR_SHARE = "Steady Fair Share";
  static final String INSTANTANEOUS_FAIR_SHARE = "Instantaneous Fair Share";
  @RequestScoped
  static class FSQInfo {
    FairSchedulerQueueInfo qinfo;
  }
  
  static class LeafQueueBlock extends HtmlBlock {
    final FairSchedulerLeafQueueInfo qinfo;

    @Inject LeafQueueBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerLeafQueueInfo)info.qinfo;
    }

    @Override
    protected void render(Block html) {
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          __("Used Resources:", qinfo.getUsedResources().toString()).
          __("Demand Resources:", qinfo.getDemandResources().toString()).
          __("AM Used Resources:", qinfo.getAMUsedResources().toString()).
          __("AM Max Resources:", qinfo.getAMMaxResources().toString()).
          __("Num Active Applications:", qinfo.getNumActiveApplications()).
          __("Num Pending Applications:", qinfo.getNumPendingApplications()).
          __("Min Resources:", qinfo.getMinResources().toString()).
          __("Max Resources:", qinfo.getMaxResources().toString()).
          __("Max Container Allocation:",
              qinfo.getMaxContainerAllocation().toString()).
          __("Reserved Resources:", qinfo.getReservedResources().toString());
      int maxApps = qinfo.getMaxApplications();
      if (maxApps < Integer.MAX_VALUE) {
        ri.__("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri.__(STEADY_FAIR_SHARE + ":", qinfo.getSteadyFairShare().toString());
      ri.__(INSTANTANEOUS_FAIR_SHARE + ":", qinfo.getFairShare().toString());
      ri.__("Preemptable:", qinfo.isPreemptable());
      html.__(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
    }
  }
  
  static class ParentQueueBlock extends HtmlBlock {
	    final FairSchedulerQueueInfo qinfo;

    @Inject ParentQueueBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerQueueInfo)info.qinfo;
    }

    @Override
    protected void render(Block html) {
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          __("Used Resources:", qinfo.getUsedResources().toString()).
          __("Min Resources:", qinfo.getMinResources().toString()).
          __("Max Resources:", qinfo.getMaxResources().toString()).
          __("Max Container Allocation:",
              qinfo.getMaxContainerAllocation().toString()).
          __("Reserved Resources:", qinfo.getReservedResources().toString());
      int maxApps = qinfo.getMaxApplications();
      if (maxApps < Integer.MAX_VALUE) {
        ri.__("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri.__(STEADY_FAIR_SHARE + ":", qinfo.getSteadyFairShare().toString());
      ri.__(INSTANTANEOUS_FAIR_SHARE + ":", qinfo.getFairShare().toString());
      html.__(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
    }
  }

  static class QueueBlock extends HtmlBlock {
    final FSQInfo fsqinfo;

    @Inject QueueBlock(FSQInfo info) {
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      Collection<FairSchedulerQueueInfo> subQueues = fsqinfo.qinfo.getChildQueues();
      UL<Hamlet> ul = html.ul("#pq");
      for (FairSchedulerQueueInfo info : subQueues) {
        float capacity = info.getMaxResourcesFraction();
        float steadyFairShare = info.getSteadyFairShareMemoryFraction();
        float instantaneousFairShare = info.getFairShareMemoryFraction();
        float used = info.getUsedMemoryFraction();
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(capacity * Q_MAX_WIDTH)).
              $title(join(join(STEADY_FAIR_SHARE + ":", percent(steadyFairShare)),
                  join(" " + INSTANTANEOUS_FAIR_SHARE + ":", percent(instantaneousFairShare)))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(steadyFairShare / capacity))).
            __('.').__().
              span().$style(join(Q_INSTANTANEOUS_FS, ";font-size:1px;",
                  width(instantaneousFairShare/capacity))).
            __('.').__().
              span().$style(join(width(used/capacity),
                ";font-size:1px;left:0%;", used > instantaneousFairShare ? Q_OVER : Q_UNDER)).
            __('.').__().
              span(".q", info.getQueueName()).__().
            span().$class("qstats").$style(left(Q_STATS_POS)).
            __(join(percent(used), " used")).__();

        fsqinfo.qinfo = info;
        if (info instanceof FairSchedulerLeafQueueInfo) {
          li.ul("#lq").li().__(LeafQueueBlock.class).__().__();
        } else {
          li.ul("#lq").li().__(ParentQueueBlock.class).__().__();
          li.__(QueueBlock.class);
        }
        li.__();
      }

      ul.__();
    }
  }
  
  static class QueuesBlock extends HtmlBlock {
    final FairScheduler fs;
    final FSQInfo fsqinfo;
    
    @Inject QueuesBlock(ResourceManager rm, FSQInfo info) {
      fs = (FairScheduler)rm.getResourceScheduler();
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      html.__(MetricsOverviewTable.class);
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
          __("Application Queues").__().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();
      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(Q_END).__("100% ").__().
              span(".q", "default").__().__();
      } else {
        FairSchedulerInfo sinfo = new FairSchedulerInfo(fs);
        fsqinfo.qinfo = sinfo.getRootQueueInfo();
        float used = fsqinfo.qinfo.getUsedMemoryFraction();

        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold").__("Legend:").__().
            span().$class("qlegend ui-corner-all").$style(Q_GIVEN).
              $title("The steady fair shares consider all queues, " +
                  "both active (with running applications) and inactive.").
            __(STEADY_FAIR_SHARE).__().
            span().$class("qlegend ui-corner-all").$style(Q_INSTANTANEOUS_FS).
              $title("The instantaneous fair shares consider only active " +
                  "queues (with running applications).").
            __(INSTANTANEOUS_FAIR_SHARE).__().
            span().$class("qlegend ui-corner-all").$style(Q_UNDER).
            __("Used").__().
            span().$class("qlegend ui-corner-all").$style(Q_OVER).
            __("Used (over fair share)").__().
            span().$class("qlegend ui-corner-all ui-state-default").
            __("Max Capacity").__().
            __().
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER)).__(".").__().
              span(".q", "root").__().
            span().$class("qstats").$style(left(Q_STATS_POS)).
            __(join(percent(used), " used")).__().
            __(QueueBlock.class).__();
      }
      ul.__().__().
      script().$type("text/javascript").
          __("$('#cs').hide();").__().__().
          __(FairSchedulerAppsBlock.class);
    }
  }

  @Override
  protected void postHead(Page.HTML<__> html) {
    html.
            style().$type("text/css").
            __("#cs-tree { padding: 5px; font-family: Arial, sans-serif; font-size: 14px; }",
                    /* Dynamic Height for the queue-wrapper div*/
                    ".wunderbaum .wb-list-container { height: auto !important; max-height: 800px !important; overflow: visible !important;}",

                    /* Remove the Wunderbaum border */
                    ".wunderbaum { border: 0px !important; color: black !important; }",

                    /* Title (first) column styling */
                    "span.wb-node.wb-col { font-weight: bold; color: #2c3e50; }",

                    /* Give the 3rd column bold for queue table row*/
                    "#cs-tree > div.wb-list-container > div > div.queue-table-row > span:nth-child(4){ font-weight: bold; }",

                    ".wb-row { width: 2500px !important; display: flex; align-items: center; }",
                    ".wb-row.queue-header:hover{ background-color: #ddd2d2 !important; }",

                    /* Alternate row colors */
                    ".wb-row.queue-table-row:nth-child(even) { background-color: rgba(0, 0, 255, 0.05); }",

                    /* Remove cell borders */
                    ".wb-row, .wb-col { border: none !important; }",
                    /* Hide the Header column and the file icon at the node's row*/
                    ".wb-header, .wb-icon.bi-file-earmark { display: none !important; }",

                    /* First row header styles */
                    "div.wb-row.first-row-header { background-color: #e6e6e6 !important; border: 1px solid !important; }",

                    /* Changing the plus icon to minus icon when open*/
                    "i.wb-icon.bi.bi-folder2::before { content: \"\\F4FE\" !important; color: #4e6879 !important; font-size: 14px !important; }",
                    "i.wb-icon.bi.bi-folder2-open::before { content: \"\\F3A3\" !important; color: #4e6879 !important; font-size: 14px !important; }",

                    /* Setting the Queue's Row background according to the UsedQueue Percentage*/
                    ".queue-header {",
                    "    position: relative;",
                    "    background-color: #e6e6e6;",
                    "    width: var(--full-width) !important;",
                    "    border: 1px solid #d3d3d3 !important;",
                    "    border-radius: 10px !important;",
                    "    /* Default overfill percentage*/",
                    "    --overfill-percentage: 0%;",

                    "    /* Fallback for when there is no dashed border set*/",
                    "    background-image: linear-gradient(to right,",
                    "        var(--fill-color) calc(var(--show-dash) * var(--fill-percentage)), /* show-dash is 0 or 1 to not show the background when there is dashed line*/",
                    "        var(--fill-color) calc(var(--show-dash) * (var(--fill-percentage) + var(--overfill-percentage))),",
                    "        transparent calc(var(--show-dash) * (var(--fill-percentage) + var(--overfill-percentage)))",
                    "    );",
                    "}",
                    ".queue-header::before {",
                    "    content: '';",
                    "    position: absolute;",
                    "    width: var(--dash-width);",
                    "    height: 100%;",
                    "    background-image: linear-gradient(to right,",
                    "        var(--fill-color) var(--fill-percentage),",
                    "        transparent var(--fill-percentage)",
                    "    );",
                    "    border: var(--dash-border, none);",
                    "    border-radius: 10px var(--dash-border-radius-top-right, 10px) var(--dash-border-radius-bottom-right, 10px) 10px;",
                    "}",
                    ".queue-header::after {",
                    "   content: '';",
                    "   position: absolute;",
                    "   left: var(--dash-width);",
                    "   width: calc(var(--overfill-percentage)/10); /* divide by 10 to shortening the width of overfill-percentage*/",
                    "   height: 100%;",
                    "   background: var(--fill-color);",
                    "}",
                    ".legend-leaf { margin: 0.5em 0 0.5em 0.5em; line-height: 18px;}",
                    ".qlegend {padding: 0 1em; margin: 1em; border-radius: 3px;}"
            ).__().
            link().$rel("stylesheet").$href("/static/bootstrap-icons.css").__().
            link().$rel("stylesheet").$href("/static/wunderbaum.css").__().
            script().$type("text/javascript").$src("/static/jt/wunderbaum.umd.min.js").__().
            script().$type("text/javascript").
            __("document.addEventListener('DOMContentLoaded', function() {",
                    "  console.log('cs div element:', document.getElementById('cs').outerHTML);",
                    "  function parseHtmlToJson(element, parentQueue = null, seenQueues){",
                    "       const queue = { name: '', usedQueue: '', title: null, backgroundColor: null, dashedWidth: null, fullWidth: null, subQueues: [] };",
                    "",
                    "        const qElement = element.querySelector('a.ui-state-default span.q'); // Extract queue name",
                    "        if (qElement) queue.name = qElement.textContent.trim();",
                    "        if (seenQueues.has(queue.name)){ return null; } // Prevent duplicate insertion queue name",
                    "        seenQueues.add(queue.name);",
                    "",
                    "        // Extract background color from the queue's spans",
                    "        const aElement = element.querySelector('a.ui-state-default');",
                    "        if (aElement){",
                    "            queue.title = aElement.title;",
                    "            queue.fullWidth = aElement.style.width;",
                    "            const spans = aElement.querySelectorAll('span:not(.q)');", // Exclude the queue name's span
                    "            for (const span of spans){",
                    "                 if (span.style.border){",
                    "                     queue.dashedWidth = span.style.width;",
                    "                 }",
                    "                 const bgColor = span.style.background;",
                    "                 if (bgColor !== 'none'){",
                    "                     queue.backgroundColor = bgColor;",
                    "                     break;",
                    "                 }",
                    "             }",
                    "        }",
                    "",
                    "        const qStates = element.querySelector('span.qstats'); // Extract used percentage",
                    "        if (qStates) queue.usedQueue = qStates.textContent.trim().replace(/used/i, 'Used');",
                    "",
                    "        // Process SubQueues",
                    "        const subQueueContainers = element.querySelectorAll('ul#pq, ul#lq');",
                    "        subQueueContainers.forEach(ul => {",
                    "           ul.querySelectorAll('li').forEach(li => {",
                    "              if (li.querySelector('a.ui-state-default')) {",
                    "                  const subQueue = parseHtmlToJson(li, queue, seenQueues);",
                    "                  if (subQueue) queue.subQueues.push(subQueue);",
                    "              }",
                    "           });",
                    "        });",
                    "",
                    "        // Extract queue details if available",
                    "        let queueDetails = {};",
                    "        const infoWrap = element.querySelector('.info-wrap');",
                    "        if (infoWrap){",
                    "           infoWrap.querySelectorAll('tr').forEach(tr => {",
                    "               const th = tr.querySelector('th');",
                    "               const td = tr.querySelector('td');",
                    "               if (th && td){",
                    "                   const key = th.textContent.trim().replace(/:$/, ''); // remove trailing colon",
                    "                   queueDetails[key] = td.textContent.trim();",
                    "               }",
                    "            });",
                    "        }",
                    "",
                    "        // Only attach details to current queue if it's a leaf node",
                    "        if (queue.subQueues.length === 0){",
                    "           queue.queueDetails = queueDetails;",
                    "        }",
                    "",
                    "        return queue;",
                    "  }",
                    "  const queueJson = { queues: [parseHtmlToJson(document.getElementById('cs'), null, new Set())] };",
                    "  console.log('Convert HTML to Queue JSON:', JSON.stringify(queueJson, null, 2));",
                    "",
                    "  function transformJsonToWunderbaumTreeData(queueData) {",
                    "         const treeData = {",
                    "            title: queueData.name,",
                    "            usedQueue: queueData.usedQueue || '',",
                    "            nodeTitle: queueData?.title,",
                    "            backgroundColor: queueData.backgroundColor,",
                    "            dashedWidth: queueData?.dashedWidth,",
                    "            fullWidth: queueData?.fullWidth,",
                    "            classes: 'queue-header',",
                    "            children: []",
                    "          };",
                    "",
                    "          if (queueData.queueDetails){",
                    "               treeData.children.push(",
                    "                   {",
                    "                       title: '',",
                    "                       column8: `${queueData.name} Queue Status`,",
                    "                       classes: 'first-row-header'",
                    "                    },",
                    "                    ...Object.entries(queueData.queueDetails).map(([key, value]) => ({",
                    "                        title: '',",
                    "                        column3: key, // To put the value in the centers",
                    "                        column4: value,",
                    "                        classes: 'queue-table-row'",
                    "                    }))",
                    "               );",
                    "          }",
                    "",
                    "         // Recursively process subQueues",
                    "         if (queueData.subQueues && queueData.subQueues.length > 0){",
                    "            queueData.subQueues.forEach(subQueue => {",
                    "                treeData.children.push(transformJsonToWunderbaumTreeData(subQueue));",
                    "            });",
                    "         }",
                    "",
                    "         return treeData;",
                    "   }",
                    "   const wunderbaumTreeData = queueJson.queues.map(queueData => transformJsonToWunderbaumTreeData(queueData));",
                    "   console.log('Wunderbaum Tree Data:', JSON.stringify(wunderbaumTreeData, null, 2));",
                    "",
                    "   let csDiv = document.getElementById('cs');",
                    "   csDiv.innerHTML = `",
                    "     <div id='queue-wrapper'>",
                    "       <div id='cs-tree'></div>",
                    "     </div>",
                    "   `;",
                    "   let wrapper = document.getElementById('queue-wrapper');",
                    "   let legendDiv = document.createElement('div');",
                    "   legendDiv.className = 'legend-leaf';",
                    "   legendDiv.innerHTML = `",
                    "        <span style='font-weight: bold'>Legend:</span>",
                    "        <span class='qlegend' style='background:none;border:1px solid #000000' ",
                    "              title='The steady fair shares consider all queues, both active (with running applications) and inactive.'",
                    "        > Steady Fair Share </span>",
                    "        <span class='qlegend' style='background:none;border:1px dashed #000000'",
                    "              title='The instantaneous fair shares consider only active queues (with running applications).'",
                    "        >Instantaneous Fair Share</span>",
                    "        <span class='qlegend' style='background:#5BD75B'>Used</span>",
                    "        <span class='qlegend' style='background:#FFA333'>Used (over fair share)</span>",
                    "        <span class='qlegend' style='border: 1px solid #d3d3d3; background: #e6e6e6'>Max Capacity</span>",
                    "   `;",
                    "   wrapper.prepend(legendDiv);",
                    "",
                    "   // Set up the initial queues for URL manipulation",
                    "   const urlParams = new URLSearchParams(window.location.search);",
                    "   const firstQueue = urlParams.get('openQueues');",
                    "   const firstQueues = firstQueue ? [firstQueue] : [];",
                    "   const hashFragment = window.location.hash.substring(1); // remove leading '#' by starting from index 1",
                    "   const hashQueues = hashFragment ? hashFragment.split('#') : [];",
                    "   let openQueues = [...firstQueues, ...hashQueues];",
                    "",
                    "   const wbTree = new mar10.Wunderbaum({",
                    "     element: document.getElementById('cs-tree'),",
                    "     navigationModeOption: 'row',",
                    "     source: wunderbaumTreeData,",
                    "     columns: [",
                    "       { id: '*', title: 'Property', width: '300px' },",
                    "       { id: 'column1', title: 'Column1', width: '300px' },",
                    "       { id: 'column2', title: 'Column2', width: '100px' },",
                    "       { id: 'column3', title: 'Column3', width: '500px' },",
                    "       { id: 'column4', title: 'Column4', width: '400px' },",
                    "       { id: 'column5', title: 'Column5', width: '200px' },",
                    "       { id: 'column6', title: 'Column6', width: '200px' },",
                    "       { id: 'column7', title: 'Column7', width: '300px' },",
                    "       { id: 'column8', title: 'Column8', width: '200px' },",
                    "     ],",
                    "     scrollParent: document.getElementById('queue-wrapper'),",
                    "     activate: ({ node }) => {",
                    "       // Responsible for displaying the running application when click on specific queue",
                    "       if (node.hasClass('queue-header')){",
                    "           console.log(`Clicked queue: ${node.title}\\nUsed resources: ${node.data.usedQueue}`);",
                    "           const queueHeaderChildren = node.children.filter(child => child.hasClass('queue-header')); // count the 'root...' children only",
                    "           const q = `^${node.title}${queueHeaderChildren.length === 0 ? '$' : '\\\\.' }`;",
                    "           const dataTable = $('#apps').DataTable();",
                    "           dataTable.search('');",
                    "           dataTable.columns().every(function() { this.search(''); }); // Clear existing filtering",
                    "           // Update this filter column index for queue if new columns are added",
                    "           // Current index for queue column is 4",
                    "           dataTable.column(4).search(q, true, false).draw(); // 'true' to treat q as regex not plain text and 'false' for not enable smart match",
                    "           // Log filtered rows",
                    "           dataTable.rows({ search: 'applied' }).every(function() {",
                    "              console.log('Matched row:', this.data());",
                    "           });",
                    "       }",
                    "     },",
                    "     expand: ({ node }) => {",
                    "       const queueName = node.title;",
                    "       if (node.isExpanded()){",
                    "           if(!openQueues.includes(queueName)) openQueues.push(queueName);",
                    "       } else {",
                    "           openQueues = openQueues.filter(q => q !== queueName);",
                    "       }",
                    "",
                    "       // Update first queue param",
                    "       openQueues.length ? urlParams.set('openQueues', openQueues[0]) : urlParams.delete('openQueues');",
                    "       // Update fragment parts",
                    "       const hashPart = openQueues.slice(1).join('#');",
                    "       const newHash = hashPart ? `#${hashPart}` : '';",
                    "       history.replaceState({}, '', `?${urlParams.toString()}${newHash}`);",
                    "",
                    "     },",
                    "",
                    "     render: ({ node, renderColInfosById}) => {",
                    "         Object.values(renderColInfosById).forEach(col => {",
                    "           if (col.id === 'column7' && node.data.usedQueue){",
                    "               col.elem.textContent = node.data.usedQueue; // To take usedQueue Data display at column7",
                    "           } else { ",
                    "               col.elem.textContent = node.data[col.id] || ''; // to fill in the value in each column",
                    "           }",
                    "         });",
                    "",
                    "         if (node.hasClass('queue-header')){ // For the Used Queue Percentage Highlight",
                    "             const usedPercentage = node.data.usedQueue.split('%')[0].trim();",
                    "",
                    "             if (usedPercentage > 100){",
                    "                const overfillPercentage = usedPercentage -100;",
                    "                node._rowElem.style.setProperty('--dash-border-radius-top-right', 0);",
                    "                node._rowElem.style.setProperty('--dash-border-radius-bottom-right', 0);",
                    "                node._rowElem.style.setProperty('--overfill-percentage', `${overfillPercentage}%`);",
                    "             }",
                    "             node._rowElem.title = node.data.nodeTitle;",
                    "             node._rowElem.style.setProperty('--full-width', node.data.fullWidth); /* fullWidth Acts as the maximum capacity*/",
                    "             node._rowElem.style.setProperty('--fill-color', node.data.backgroundColor);",
                    "             node._rowElem.style.setProperty('--fill-percentage', `${usedPercentage}%`);",
                    "         }",
                    "",
                    "         if (node.data.dashedWidth != null){ /* Dashed width acts as the Capacity*/",
                    "             node._rowElem.style.setProperty('--dash-width', node.data.dashedWidth); /* dashedWidth Acts as the Instantaneous Fair Share*/",
                    "             node._rowElem.style.setProperty('--dash-border', '1px dashed #BFBFBF');",
                    "             node._rowElem.style.setProperty('--show-dash', '0'); /* Disable the queue-header fill-in background when there is dashed width*/",
                    "         } else {",
                    "             node._rowElem.style.setProperty('--dash-border', 'none');",
                    "             node._rowElem.style.setProperty('--show-dash', '1'); /* Fall back to the queue-header fill-in background when there is no dashed-width*/",
                    "         }",
                    "      },",
                    "   });",
                    "",
                    "   // Handle Initial Open Queues from URL",
                    "   wbTree.visit((node) => {",
                    "      if (openQueues.includes(node.title)){",
                    "         node.setExpanded(true);",
                    "      }",
                    "   });",
                    "   document.getElementById('cs').style.display = 'block';",
                    "});"
            ).__();
  }
  
  @Override protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }

  @Override
  protected String initAppsTable() {
    return WebPageUtils.appsTableInit(true, false);
  }

  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}
