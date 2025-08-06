require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

// Инициализация Supabase клиента
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Конфигурация
const WB_API_KEY = process.env.WB_API_KEY;
const WB_API_URL = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders';
const SYNC_TABLE = 'wb_orders'; // Таблица для заказов
const STATE_TABLE = 'sync_state'; // Таблица для состояния синхронизации
const SYNC_ID = 'wb_orders_sync'; // ID вашей синхронизации
const BATCH_SIZE = 500; // Размер пакета для вставки
const REQUEST_DELAY = 1000; // Задержка между запросами (1 сек)

// Основная функция синхронизации
async function syncOrders() {
  try {
    console.log('🚀 Запуск синхронизации данных Wildberries → Supabase');
    
    // 1. Получаем последнюю дату синхронизации
    const lastSyncDate = await getLastSyncDate();
    console.log(`⏱ Последняя дата синхронизации: ${lastSyncDate || 'нет данных'}`);
    
    // 2. Загружаем данные из WB с пагинацией
    let allOrders = [];
    let currentDate = lastSyncDate;
    let page = 1;
    let hasMoreData = true;
    
    while (hasMoreData) {
      console.log(`📡 Страница ${page}: Загрузка данных с ${currentDate}`);
      
      const orders = await fetchWbOrders(currentDate);
      
      if (orders.length === 0) {
        console.log('ℹ️ Новые данные отсутствуют');
        hasMoreData = false;
        break;
      }
      
      allOrders = [...allOrders, ...orders];
      console.log(`📦 Получено ${orders.length} записей (Всего: ${allOrders.length})`);
      
      // Обновляем дату для следующего запроса
      currentDate = getMaxDate(orders);
      page++;
      
      // Пауза для соблюдения лимитов API
      await delay(REQUEST_DELAY);
    }
    
    // 3. Сохраняем данные в Supabase
    if (allOrders.length > 0) {
      await saveToSupabase(allOrders);
      console.log(`💾 Сохранено заказов: ${allOrders.length}`);
    }
    
    // 4. Обновляем состояние синхронизации
    await updateSyncState(currentDate);
    console.log('✅ Синхронизация успешно завершена!');
    
  } catch (error) {
    console.error('⛔ Критическая ошибка:', error.message);
    if (error.response) {
      console.error('Детали ошибки:', {
        status: error.response.status,
        data: error.response.data
      });
    }
  }
}

// ======= Вспомогательные функции =======

// Задержка выполнения
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Получение последней даты синхронизации
async function getLastSyncDate() {
  try {
    const { data, error } = await supabase
      .from(STATE_TABLE)
      .select('last_sync_date')
      .eq('id', SYNC_ID)
      .single();

    if (error && error.code !== 'PGRST116') { // Игнорируем "строка не найдена"
      throw error;
    }
    
    return data?.last_sync_date || '2023-01-01T00:00:00Z';
  } catch (error) {
    console.error('Ошибка при получении состояния:', error.message);
    return '2023-01-01T00:00:00Z';
  }
}

// Обновление состояния синхронизации
async function updateSyncState(lastDate) {
  try {
    const { error } = await supabase
      .from(STATE_TABLE)
      .upsert(
        { 
          id: SYNC_ID, 
          last_sync_date: lastDate,
          last_sync: new Date().toISOString() 
        },
        { onConflict: 'id' }
      );

    if (error) throw error;
    console.log(`🔄 Состояние синхронизации обновлено: ${lastDate}`);
  } catch (error) {
    console.error('Ошибка обновления состояния:', error.message);
  }
}

// Получение максимальной даты из массива заказов
function getMaxDate(orders) {
  return orders.reduce((max, order) => 
    order.lastChangeDate > max ? order.lastChangeDate : max, 
    orders[0]?.lastChangeDate || ''
  );
}

// Загрузка заказов из Wildberries API
async function fetchWbOrders(dateFrom) {
  try {
    const response = await axios.get(WB_API_URL, {
      params: { dateFrom },
      headers: { 
        Authorization: WB_API_KEY,
        'Accept-Encoding': 'gzip,deflate,compress' 
      },
      timeout: 30000
    });

    return response.data || [];
  } catch (error) {
    console.error('🚨 Ошибка запроса к WB API:', error.message);
    
    // Автоматический повтор через 10 сек при ошибках сети
    if (error.code === 'ECONNABORTED' || !error.response) {
      console.log('🔄 Повтор запроса через 10 сек...');
      await delay(10000);
      return fetchWbOrders(dateFrom);
    }
    
    // Обработка HTTP ошибок
    if (error.response) {
      console.error(`Статус: ${error.response.status}`, error.response.data);
      
      // При ошибке 429 (слишком много запросов) - пауза
      if (error.response.status === 429) {
        const retryAfter = error.response.headers['retry-after'] || 60;
        console.log(`⏳ Лимит запросов. Пауза ${retryAfter} сек...`);
        await delay(retryAfter * 1000);
        return fetchWbOrders(dateFrom);
      }
    }
    
    return [];
  }
}

// Сохранение данных в Supabase с пакетной обработкой
async function saveToSupabase(orders) {
  try {
    console.log(`📤 Начало сохранения ${orders.length} записей...`);
    
    const transformedOrders = orders.map(order => ({
      date: convertDate(order.date),
      last_change_date: convertDate(order.lastChangeDate),
      warehouse_name: order.warehouseName,
      warehouse_type: order.warehouseType,
      country_name: order.countryName,
      oblast_okrug_name: order.oblastOkrugName,
      region_name: order.regionName,
      supplier_article: order.supplierArticle,
      nm_id: order.nmId,
      barcode: order.barcode,
      category: order.category,
      subject: order.subject,
      brand: order.brand,
      tech_size: order.techSize,
      income_id: order.incomeID,
      is_supply: order.isSupply,
      is_realization: order.isRealization,
      total_price: order.totalPrice,
      discount_percent: order.discountPercent,
      spp: order.spp,
      finished_price: order.finishedPrice,
      price_with_disc: order.priceWithDisc,
      is_cancel: order.isCancel,
      cancel_date: order.cancelDate !== '0001-01-01T00:00:00' 
        ? convertDate(order.cancelDate) 
        : null,
      sticker: order.sticker,
      g_number: order.gNumber,
      srid: order.srid
    }));

    // Пакетная обработка
    for (let i = 0; i < transformedOrders.length; i += BATCH_SIZE) {
      const batch = transformedOrders.slice(i, i + BATCH_SIZE);
      
      const { error } = await supabase
        .from(SYNC_TABLE)
        .upsert(batch, { onConflict: 'srid' });
      
      if (error) {
        console.error(`⚠️ Ошибка пакета ${i}-${i + batch.length}:`, error.message);
        // Повторная попытка для текущего пакета
        await supabase.from(SYNC_TABLE).upsert(batch, { onConflict: 'srid' });
      } else {
        console.log(`✓ Пакет ${i}-${i + batch.length} сохранен`);
      }
      
      // Небольшая пауза между пакетами
      await delay(300);
    }
    
  } catch (error) {
    console.error('Ошибка сохранения данных:', error.message);
    throw error;
  }
}

// Преобразование даты в ISO формат
function convertDate(dateString) {
  if (!dateString) return null;
  // Если дата уже в ISO формате
  if (dateString.includes('T') && dateString.endsWith('Z')) {
    return dateString;
  }
  // Добавляем временную зону, если отсутствует
  return new Date(dateString + (dateString.endsWith('Z') ? '' : 'Z')).toISOString();
}

// Запуск синхронизации
syncOrders();