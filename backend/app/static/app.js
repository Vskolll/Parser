const subcategoriesEl = document.getElementById('subcategories');
const parseBtn = document.getElementById('parseBtn');
const downloadBtn = document.getElementById('downloadBtn');
const statusEl = document.getElementById('status');
const maxItemsInput = document.getElementById('maxItems');
const previewBody = document.querySelector('#preview tbody');
const recheckFile = document.getElementById('recheckFile');
const recheckBtn = document.getElementById('recheckBtn');
const recheckStatus = document.getElementById('recheckStatus');

const selectedCategory = { name: 'Torget', url: 'https://www.finn.no/bap/browse.html' };
let selectedSubcategory = null;
let lastParams = null;

function setStatus(message) {
  statusEl.textContent = message;
}

function setRecheckStatus(message) {
  recheckStatus.textContent = message;
}

function renderList(container, items, onSelect) {
  container.innerHTML = '';
  items.forEach(item => {
    const button = document.createElement('button');
    button.className = 'list-item';
    button.textContent = item.name;
    button.addEventListener('click', () => onSelect(item, button));
    container.appendChild(button);
  });
}

function clearActive(container) {
  container.querySelectorAll('.list-item.active').forEach(el => el.classList.remove('active'));
}

async function loadSubcategories() {
  setStatus('Загружаю подкатегории Torget...');
  const res = await fetch('/api/torget-subcategories');
  const data = await res.json();
  renderList(subcategoriesEl, data.items || [], (item, button) => {
    clearActive(subcategoriesEl);
    button.classList.add('active');
    selectedSubcategory = item;
    parseBtn.disabled = false;
  });
  setStatus('Подкатегории загружены.');
}

function clearPreview() {
  previewBody.innerHTML = '';
}

function fillPreview(rows) {
  clearPreview();
  rows.slice(0, 20).forEach(row => {
    const tr = document.createElement('tr');
    const status = (row.status || '').toLowerCase();
    if (status.includes('solgt')) tr.classList.add('status-solgt');
    else if (status.includes('reservert')) tr.classList.add('status-reservert');
    else if (status.includes('inaktiv')) tr.classList.add('status-inaktiv');
    else if (status.includes('404')) tr.classList.add('status-404');
    const imgHtml = row.image
      ? `<a href="${row.image}" target="_blank" rel="noopener"><img src="${row.image}" alt="Фото" class="thumb" /></a>`
      : '';
    tr.innerHTML = `
      <td>${row.category || ''}</td>
      <td>${row.subcategory || ''}</td>
      <td>${row.title || ''}</td>
      <td>${imgHtml}</td>
      <td>${row.price || ''}</td>
      <td>${row.status || ''}</td>
    `;
    previewBody.appendChild(tr);
  });
}

async function parseListings() {
  if (!selectedSubcategory) return;
  setStatus('Парсю объявления...');
  parseBtn.disabled = true;
  downloadBtn.disabled = true;

  const form = new FormData();
  form.append('category_name', selectedCategory?.name || '');
  form.append('subcategory_name', selectedSubcategory?.name || '');
  form.append('subcategory_url', selectedSubcategory?.url || '');
  form.append('max_items', maxItemsInput.value || '50');
  form.append('preview', '1');

  const res = await fetch('/api/parse', { method: 'POST', body: form });
  const data = await res.json();
  fillPreview(data.items || []);
  if (!data.items || data.items.length === 0) {
    lastParams = null;
    downloadBtn.disabled = true;
    setStatus('Ничего не нашёл. Попробуй другую подкатегорию.');
  } else {
    lastParams = {
      category_name: selectedCategory?.name || '',
      subcategory_name: selectedSubcategory?.name || '',
      subcategory_url: selectedSubcategory?.url || '',
      max_items: maxItemsInput.value || '50',
    };
    downloadBtn.disabled = false;
    setStatus('Готово. Можно скачать XLSX.');
  }
  parseBtn.disabled = false;
}

async function downloadXlsx() {
  if (!lastParams) return;
  const form = new FormData();
  Object.entries(lastParams).forEach(([key, value]) => form.append(key, value));
  const res = await fetch('/api/parse', { method: 'POST', body: form });
  const blob = await res.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'finn_listings.xlsx';
  document.body.appendChild(a);
  a.click();
  a.remove();
  setStatus('XLSX скачан.');
}

async function recheck() {
  if (!recheckFile.files.length) {
    setRecheckStatus('Выберите XLSX файл.');
    return;
  }
  setRecheckStatus('Проверяю изменения...');

  const form = new FormData();
  form.append('file', recheckFile.files[0]);
  form.append('max_items', maxItemsInput.value || '50');

  const res = await fetch('/api/recheck', { method: 'POST', body: form });
  if (res.headers.get('content-type')?.includes('application/json')) {
    const data = await res.json();
    setRecheckStatus(data.message || 'Изменений нет.');
  } else {
    const blob = await res.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'finn_changes.xlsx';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setRecheckStatus('Файл изменений скачан.');
  }
}

parseBtn.addEventListener('click', () => parseListings());
downloadBtn.addEventListener('click', () => downloadXlsx());
recheckBtn.addEventListener('click', () => recheck());

loadSubcategories();
