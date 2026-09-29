"""Synchronize existing documentation for the a00 workbench UI."""
from pathlib import Path

root = Path(__file__).resolve().parents[2]
for relative in ("AGENTS.md", "02_Futures_Lakehouse/AGENTS.md", "02_Futures_Lakehouse/README.md", "02_Futures_Lakehouse/operations/AGENTS.md", "02_Futures_Lakehouse/operations/README.md"):
    path = root / relative
    source = path.read_text(encoding="utf-8")
    source = source.replace("总控台“维护工具”", "总控台“采集工作台”的 a00 分组")
    source = source.replace("完整检查和同步可在维护工具中人工运行", "完整检查和同步可在采集工作台的 a00 分组人工运行")
    source = source.replace("PySide6 交互总控台、原参数透传", "PySide6 采集工作台、a00 独立看板、原参数透传")
    if relative == "02_Futures_Lakehouse/README.md":
        source = source.replace("工具页只解释职责并打开源码", "各自的工作页只解释流程与职责并打开源码")
    if relative == "02_Futures_Lakehouse/operations/AGENTS.md":
        source = source.replace("- 采集配置只列出 a01—a04 直接子级、具有同名 Notebook 的 b*.py 正式入口；", "- “采集工作台”统一展示 a00 准备与检查和 a01—a04 业务分组。a00 只能单项运行白名单工具或浏览共用模块，不进入批量勾选与日常配置。业务列表只列出 a01—a04 直接子级、具有同名 Notebook 的 b*.py 正式入口；")
        source = source.replace("- “维护工具”只允许运行固定命令：", "- 工作台 a00 分组只允许运行固定命令：")
        source = source.replace("维护页只说明职责并打开源码", "各自独立工作页只说明流程与职责并打开源码")
        anchor = "## 维护工具与共用模块\n"
        source = source.replace(anchor, anchor + "\n- a00 首批提供环境检查、Windows 控制面检查、代码检查、完整检查、导出同步五个执行看板及 a00_03/a00_04 两个流程说明页。启动 a00 单项后留在所属看板；a01—a04 本轮继续使用原参数与通用监控。不得因页面导航或测试请求执行采集或正式同步。\n- 执行看板同时显示流程位置、各步骤独立进度条、本步总量/已处理/剩余、异常项目、逐依赖/逐文件结果、耗时、心跳与增量日志。计数只来自脚本的结构化事件；已处理包含失败项，不等于成功量，步骤完成数不代表耗时百分比。无事件与旧历史保持未知；中断或失败保留最后位置。\n- 环境检查按解释器与依赖展示；控制面检查按原子长路径、共享读取、Arrow 往返展示；a00_02 分别展示生成、可选写入、复核，逐文件结果独立留存。a00 切换按工具绑定本窗口运行目录，不混用其他工具的计数或异步日志。界面顶部在所有页面持续显示本窗口活跃批次的阶段、计数、耗时、心跳和监控故障。\n")
    if relative == "02_Futures_Lakehouse/operations/README.md":
        old = "顶部切换“采集配置”“运行监控”“历史批次”“维护工具”。配置页左侧搜索和勾选环节，"
        new = "顶部切换“采集工作台”“运行监控”“历史批次”。原维护工具已合并到工作台 a00“准备与检查”，打开窗口默认进入运行环境页。a00 分组不参与采集批量勾选；左侧搜索同时覆盖 a00 与 a01—a04。业务工作页左侧勾选环节，"
        assert old in source
        source = source.replace(old, new)
        source = source.replace("## 维护工具\n\n工具页提供以下固定命令", "## a00 准备与检查\n\na00 分组提供以下固定命令")
        source = source.replace("检查工具点击“执行检查”启动。", "检查工具点击“执行检查”启动，并留在当前独立看板。")
        source = source.replace("工具页只展示职责和打开源码", "各自工作页只展示调用流程、职责与打开源码")
        anchor = "所有维护任务使用相同后台 worker、全局锁、可见监控、中断和历史证据"
        source = source.replace(anchor, "五个执行看板按实际工作流程定制：\n\n- 运行环境：解释器与环境、核心依赖两步；逐项展示依赖版本与结果。\n- Windows 控制面：长路径原子替换、共享读取与再次替换、Arrow 文件往返三步。\n- Notebook 代码检查与完整检查：生成与语法验证、正文或字节比较两步。\n- Notebook 导出同步：生成、写入、写后完整复核三步；逐文件分别展示各步结果。\n\n每页同时显示第几步/总步数、步骤独立进度条、本步总量/已处理/剩余、异常项目、耗时与心跳；下方结果表和实时原始日志可拖动分隔条调整。小窗口可滚动工作页。已处理量包含失败项，不代表通过量；异常项目与具体失败原因独立显示。无计数时显示未知，不从日志位置推算比例。中断、失败与旧历史均不会补造成功。a00_03 与 a00_04 只呈现调用流程，没有虚构的运行百分比。\n\n看板记住本窗口中各工具的运行目录，切换不会串入其他工具的状态或异步日志。历史记录继续通过“历史批次”进入通用只读监控；重开窗口不接管旧运行。窗口顶部在浏览任何页面时都持续显示本窗口活跃批次的阶段、可用计数、耗时与心跳。a01—a04 的定制看板留待逐环节建设，本轮保留原参数和通用监控。\n\n" + anchor)
        source = source.replace("& 'E:\\anaconda3\\envs\\latitude\\python.exe' 'E:\\Latitude_Analytics_v2\\02_Futures_Lakehouse\\operations\\runtime\\verify_operations_runtime.py'", "& 'E:\\anaconda3\\envs\\latitude\\python.exe' 'E:\\Latitude_Analytics_v2\\02_Futures_Lakehouse\\operations\\runtime\\verify_operations_runtime.py'\n& 'E:\\anaconda3\\envs\\latitude\\python.exe' -m unittest discover -s 'E:\\Latitude_Analytics_v2\\00_draft_collection_02\\tests' -p 'test_a00_workbench.py' -v")
    path.write_text(source, encoding="utf-8", newline="\n")

# This earlier draft visual check must follow the renamed/merged page too.
path = root / "00_draft_collection_02/scripts/verify_operations_single_tools_visual.py"
source = path.read_text(encoding="utf-8").replace('("maintenance", window.maintenance_tab)', '("maintenance", window.configure_tab)')
source = source.replace('                if label == "monitor":', '                if label == "maintenance":\n                    window.entry_tree.setCurrentItem(window.a00_items["verify_runtime"])\n                elif label == "configuration":\n                    window.entry_tree.setCurrentItem(window.entry_items["a01/b06_futures_minute"])\n                if label == "monitor":')
source = source.replace('window.tabs.setCurrentWidget(window.maintenance_tab)', 'window.tabs.setCurrentWidget(window.configure_tab)\n        window.entry_tree.setCurrentItem(window.a00_items["verify_runtime"])')
path.write_text(source, encoding="utf-8", newline="\n")
