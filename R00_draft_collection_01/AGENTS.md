# 适用范围与规范入口

本文件适用于 `R00_draft_collection_01/` 及其子目录中的实验性工作。进入本目录创建、修改或执行实验前，Agent 必须读取本文件和[根规则](../AGENTS.md)。本目录收纳探索 Notebook；文件收纳与导出规则由本文件定义，环境、根目录定位、命名、长批次和数据湖边界遵循根规则。

相关入口：[仓库 README](../README.md)、[Git 收纳规则](../.gitignore)、[环境说明](../environment/README.md)、[湖仓规则](../R02_Market_Data/a02_Lake/AGENTS.md)。修改本目录约定时，同步受影响的 Notebook、读取者、根规范索引和 README，不得留下相互矛盾的路径说明。

# 文件收纳

- Notebook 和本目录的规范文件保留在目录顶层。实验生成的数据、缓存、图片、JSON、日志和其他配套文件不得与 Notebook 平铺。
- 配套文件统一放入 `<Notebook 文件名去掉 .ipynb>/`。例如 `testing_15.ipynb` 的配套文件放入 `testing_15/`，文件名使用 `testing_15_` 前缀并说明内容。配套目录直接位于本目录下，不增加 `outputs/` 等中间层；不为同一个实验另建散落的数据目录、图片目录或结果目录。
- 输出目录只在确实需要写文件时创建，不预建空目录。路径在 Notebook 初始化或写入位置明确给出，不以修改当前工作目录隐式决定输出位置。
- `testing_N/` 配套目录保存本地实验产物，已由根 `.gitignore` 排除。不能靠隐藏文件、修改扩展名或忽略顶层散文件代替实际收纳；已跟踪文件也不会因添加忽略规则自动退出 Git。
- 尚未确认归属的临时脚本、测试和一次性工具按根规则放入 `R00_draft_collection_02/`；不要将这类工具散落在本目录顶层或仓库根目录。

目录示例：

```text
R00_draft_collection_01/
  AGENTS.md
  testing_10.ipynb
  testing_11.ipynb
  testing_15.ipynb
  testing_10/
    testing_10_rb_main_minute.parquet
  testing_15/
    testing_15_rb_results.json
```

# 默认展示与文件导出

- 图表默认直接显示并保存在 Notebook 输出中。表格和结论也优先保存在 Notebook 内，不为每幅图、每张表或每次计算自动生成独立文件。
- 独立图片、JSON、CSV、可选缓存等导出须使用一个明确的开关，例如 `EXPORT_FILES = False`，默认关闭。用户要求独立文件时，按要求开启或执行对应导出；普通 Run All 不得无条件生成这些文件。
- 确实被后续计算或其他 Notebook 读取的必要中间数据可以落盘，但必须说明生成者、读取者和路径。仅为方便 Agent 自己验证或重复查看，不构成默认长期落盘的理由。
- 不自动同时导出多个图像格式，不重复保存已经内嵌的结果，不在每次运行时追加时间戳副本。需要版本化批次时，先明确批次边界和保留用途，遵循根规则中的执行要求。
- 默认关闭导出时，不能打印“已保存”或返回未生成文件的链接。开启导出时，写入与文件路径提示必须同步受该开关控制。

在按照根规则定位项目根目录、获得 `candidate_root` 后，直接使用明确路径和库调用：

```python
notebook_dir = candidate_root / "R00_draft_collection_01"
output_dir = notebook_dir / "testing_15"
EXPORT_FILES = False

# 绘图代码生成 fig 后：
if EXPORT_FILES:
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / "testing_15_rb_volume.png", dpi=160)
plt.show()
```

不为这些简单路径和单次保存操作新增通用导出管理器或薄封装。

# 引用同步、整理与交付

- `testing_10.ipynb` 生成的分钟数据固定保存到 `testing_10/testing_10_rb_main_minute.parquet`，由 `testing_11.ipynb` 和 `testing_12.ipynb` 读取。不得恢复顶层旧路径或在草稿二区复制另一份相同数据。
- 移动配套文件时，先检查生成代码、读取代码、Markdown 和相关说明中的引用，同一次修改中同步路径；后续运行必须继续写入整理后的目录。
- 整理已有文件时核对内容完整性和引用可用性。不要把“缓存”“中间结果”或“验证已完成”视为删除授权；删除已有材料须有用户对相应范围的明确授权。
- 收尾时检查：新增产物没有落在顶层；默认运行没有额外导出；必要输入能够从新路径读取；说明和实际保存位置一致。交付说明给出 Notebook 入口及必要的输出目录，不用长串配套文件列表代替结果说明。
- 正式 silver 保持只读。实验输出属于本 Notebook，不得写入正式湖、加入稳定数据契约或复制整张来源表充当本地缓存。
