-- Ajustes para que los docs/ de GitHub se lean bien en el libro:
--  1. Enlaces relativos a código o archivos que no son páginas del libro (src/, infra/, .py, .sh,
--     .log, .docx...) se reescriben al archivo en GitHub.
--  2. Bloques ```mermaid (sintaxis de GitHub) se dibujan como diagramas (mermaid.html carga la librería).
local REPO = "https://github.com/pipaber/dataenergia-lumina/blob/main/"
local PAGINAS = { md = true, qmd = true, html = true, png = true, svg = true, jpg = true }

local function normalizar(ruta)
  local partes = {}
  for p in ruta:gmatch("[^/]+") do
    if p == ".." then table.remove(partes)
    elseif p ~= "." then table.insert(partes, p) end
  end
  return table.concat(partes, "/")
end

local function dir_relativo()
  local archivo = quarto.doc.input_file
  local proyecto = quarto.project.directory
  if not archivo or not proyecto then return "" end
  local dir = pandoc.path.directory(archivo)
  return (pandoc.path.make_relative(dir, proyecto):gsub("\\", "/"))
end

function Link(el)
  local destino = el.target
  if destino:match("^%a[%w+.-]*:") or destino:match("^#") or destino:match("^/") then return nil end
  local base = destino:gsub("#.*$", "")
  if base == "" then return nil end
  local ext = base:match("%.(%w+)$")
  if ext and PAGINAS[ext:lower()] then return nil end
  local dir = dir_relativo()
  local ruta = normalizar((dir ~= "" and dir ~= "." and (dir .. "/") or "") .. base)
  el.target = REPO .. ruta
  return el
end

function CodeBlock(el)
  if el.classes:includes("mermaid") then
    local texto = el.text:gsub("&", "&amp;"):gsub("<", "&lt;"):gsub(">", "&gt;")
    return pandoc.RawBlock("html", '<pre class="mermaid">' .. texto .. "</pre>")
  end
end
