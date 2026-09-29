// 本轮离线检查：使用本机 PyCharm 随附 Mermaid 渲染器，不访问网络。
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('C:/Users/31918/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');

async function main() {
  const notebookPath = path.resolve('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb');
  const outputDir = process.argv[2];
  if (!outputDir) throw new Error('需要本轮预览输出目录');
  fs.mkdirSync(outputDir, { recursive: true });
  const notebook = JSON.parse(fs.readFileSync(notebookPath, 'utf8'));
  const diagrams = notebook.cells
    .filter(cell => cell.cell_type === 'markdown' && cell.source.join('').includes('```mermaid'))
    .filter(cell => !process.argv[3] || cell.id === process.argv[3])
    .map(cell => ({ id: cell.id, source: cell.source.join('').match(/```mermaid\n([\s\S]*?)\n```/)[1] }));
  const browser = await chromium.launch({
    executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    headless: true,
  });
  try {
    const page = await browser.newPage({ viewport: { width: 1500, height: 1800 }, deviceScaleFactor: 1 });
    await page.route('**/*', route => route.abort());
    await page.setContent('<html><meta charset="utf-8"><style>body{margin:20px;background:white;font-family:"Microsoft YaHei",sans-serif}#diagram{display:inline-block}svg{max-width:none!important}</style><div id="diagram"></div></html>');
    await page.addScriptTag({ path: 'E:/PyCharm 2026.1.2/plugins/jupyter-plugin/jupyter-web/assets/vendor/mermaid.min.js' });
    await page.evaluate(() => mermaid.initialize({ startOnLoad: false, securityLevel: 'strict', theme: 'default', fontFamily: 'Microsoft YaHei, sans-serif' }));
    const results = [];
    for (const diagram of diagrams) {
      const rendered = await page.evaluate(async ({ id, source }) => {
        await mermaid.parse(source);
        const { svg } = await mermaid.render(id, source);
        const container = document.getElementById('diagram');
        container.innerHTML = svg;
        const root = container.querySelector('svg');
        const box = root.viewBox.baseVal;
        root.setAttribute('width', Math.ceil(box.width));
        root.setAttribute('height', Math.ceil(box.height));
        await document.fonts.ready;
        const clipped = [...root.querySelectorAll('foreignObject')].filter(node => {
          const content = node.querySelector('div');
          return content && (content.scrollWidth > node.width.baseVal.value + 2 || content.scrollHeight > node.height.baseVal.value + 2);
        }).length;
        return { svg, width: Math.ceil(box.width), height: Math.ceil(box.height), clipped };
      }, diagram);
      fs.writeFileSync(path.join(outputDir, `${diagram.id}.svg`), rendered.svg);
      await page.locator('#diagram').screenshot({ path: path.join(outputDir, `${diagram.id}.png`) });
      results.push({ id: diagram.id, width: rendered.width, height: rendered.height, clipped: rendered.clipped });
    }
    fs.writeFileSync(path.join(outputDir, 'render_results.json'), JSON.stringify(results, null, 2));
    console.log(JSON.stringify(results, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
