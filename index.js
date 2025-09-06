require('dotenv').config();

const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { Client, GatewayIntentBits, Events, Partials, EmbedBuilder } = require('discord.js');
const { createConnection } = require('mysql2/promise');
const cron = require('node-cron');
const archiver = require('archiver');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN || '';
const OWNER_ID = process.env.OWNER_ID ? String(process.env.OWNER_ID) : '';
const NOTIFY_CHANNEL_ID = process.env.NOTIFY_CHANNEL_ID || '';
const BACKUP_CHANNEL_ID = process.env.BACKUP_CHANNEL_ID || '';

const MYSQL_CONFIG = {
  host: process.env.MYSQL_HOST || 'localhost',
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || '',
};

const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 */3 * * *';

const DB_FILE = path.join(__dirname, 'db_list.json');
function loadDbList() {
  if (!fs.existsSync(DB_FILE)) return [];
  try {
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch {
    return [];
  }
}
async function saveDbList(arr) {
  await fsp.writeFile(DB_FILE, JSON.stringify(arr, null, 2), 'utf8');
}

function logTZ(msg) {
  const ts = new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
  console.log(`[${ts} WIB] ${msg}`);
}

function zipFile(sourceFile, zipPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve());
    archive.on('error', err => reject(err));

    archive.pipe(output);
    archive.file(sourceFile, { name: path.basename(sourceFile) });
    archive.finalize();
  });
}

async function dumpDatabase(dbName) {
  const conn = await createConnection({ ...MYSQL_CONFIG, database: dbName });
  const [tables] = await conn.query('SHOW TABLES');
  const tableKey = Object.keys(tables[0] || {})[0] || `Tables_in_${dbName}`;

  let dump = `-- Backup for ${dbName} @ ${new Date().toISOString()}\n\nCREATE DATABASE IF NOT EXISTS \`${dbName}\`;\nUSE \`${dbName}\`;\n\n`;

  for (const tbl of tables) {
    const tableName = tbl[tableKey];

    const [[createStmt]] = await conn.query(`SHOW CREATE TABLE \`${tableName}\``);
    dump += `${createStmt['Create Table']};\n\n`;

    const [rows] = await conn.query(`SELECT * FROM \`${tableName}\``);
    for (const row of rows) {
      const cols = Object.keys(row).map(col => `\`${col}\``).join(', ');
      const vals = Object.values(row)
        .map(val => {
          if (val === null) return 'NULL';
          if (val instanceof Date) {
            const ts = val.toISOString().replace('T', ' ').slice(0, 19);
            return `'${ts}'`;
          }
          if (Buffer.isBuffer(val)) {
            return `'${val.toString('base64')}'`;
          }
          return `'${String(val)
            .replace(/\\/g, "\\\\")
            .replace(/'/g, "\\'")}'`;
        })
        .join(', ');
      dump += `INSERT INTO \`${tableName}\` (${cols}) VALUES (${vals});\n`;
    }
    dump += '\n\n';
  }

  await conn.end();
  return dump;
}

async function listDatabases() {
  const dbs = loadDbList();
  if (!dbs || dbs.length === 0) {
    throw new Error('❌ Daftar database kosong. Set dulu pakai command !setdb db1,db2');
  }
  return dbs;
}

async function sendFileToChannel(channel, filePath, label) {
  const stat = await fsp.stat(filePath);
  const sizeMB = (stat.size / (1024 * 1024)).toFixed(2);
  const msg = `${label} (\`${sizeMB} MB\`)`;
  return channel.send({ content: msg, files: [filePath] });
}

async function backupAllDatabases(notifyChannel, fileChannel) {
  const dbNames = await listDatabases();

  let success = 0;
  let failed = 0;
  const start = Date.now();

  for (const dbName of dbNames) {
    const dateStr = new Date().toISOString().split('T')[0];
    const sqlPath = path.join(__dirname, `${dbName}_${dateStr}.sql`);
    const zipPath = path.join(__dirname, `${dbName}_${dateStr}.zip`);

    try {
      const sql = await dumpDatabase(dbName);
      await fsp.writeFile(sqlPath, sql, 'utf8');

      await zipFile(sqlPath, zipPath);
      await fsp.unlink(sqlPath);

      if (!fileChannel) throw new Error('Backup channel not set');
      await sendFileToChannel(fileChannel, zipPath, `📦 Backup \`${dbName}\``);

      success++;
    } catch (err) {
      logTZ(`❌ Failed ${dbName}: ${err.message}`);
      failed++;
      if (notifyChannel) {
        await notifyChannel.send({
          embeds: [
            new EmbedBuilder()
              .setTitle(`❌ Gagal backup: ${dbName}`)
              .setDescription('```' + (err.stack || err.message) + '```')
              .setColor(0xED4245)
              .setTimestamp(new Date()),
          ],
        }).catch(() => {});
      }
    } finally {
      try { await fsp.unlink(zipPath); } catch {}
      try { await fsp.unlink(sqlPath); } catch {}
    }
  }

  const duration = ((Date.now() - start) / 1000).toFixed(2);
  const embed = new EmbedBuilder()
    .setTitle('📦 Backup Semua Database Selesai')
    .setColor(failed > 0 ? 0xFEE75C : 0x57F287)
    .addFields(
      { name: '📁 Total Database', value: `\`${dbNames.length}\``, inline: true },
      { name: '✅ Berhasil', value: `\`${success}\``, inline: true },
      { name: '❌ Gagal', value: `\`${failed}\``, inline: true },
      { name: '⏱️ Durasi Backup', value: `\`${duration} detik\``, inline: false },
    )
    .setTimestamp(new Date());

  if (notifyChannel) await notifyChannel.send({ embeds: [embed] });
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

client.once(Events.ClientReady, (c) => {
  logTZ(`✅ Logged in as ${c.user.tag}`);

  cron.schedule(CRON_SCHEDULE, async () => {
    logTZ(`[CRON] Triggered backup`);
    try {
      const notify = await client.channels.fetch(NOTIFY_CHANNEL_ID).catch(() => null);
      const fileCh = await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null);
      await backupAllDatabases(notify, fileCh);
    } catch (err) {
      logTZ('❌ Scheduled backup error: ' + err.message);
      const notify = await client.channels.fetch(NOTIFY_CHANNEL_ID).catch(() => null);
      if (notify) {
        await notify.send({
          embeds: [
            new EmbedBuilder()
              .setTitle('❌ Error Backup Terjadwal')
              .setDescription('```' + (err.stack || err.message) + '```')
              .setColor(0xED4245)
              .setTimestamp(new Date()),
          ],
        }).catch(() => {});
      }
    }
  });
});

client.on(Events.MessageCreate, async (msg) => {
  if (msg.author.bot || !msg.content.startsWith('!')) return;
  const args = msg.content.slice(1).trim().split(/\s+/);
  const cmd = args.shift()?.toLowerCase();
  const isOwner = msg.author.id === OWNER_ID;

  if (cmd === 'ping') {
    const sent = await msg.channel.send('⏳');
    const latency = sent.createdTimestamp - msg.createdTimestamp;
    await sent.edit(`🏓 Pong! WS: \`${Math.round(client.ws.ping)}ms\`, RT: \`${latency}ms\``);
    return;
  }

  if (cmd === 'credit') {
    await msg.channel.send({
      embeds: [
        new EmbedBuilder()
          .setTitle('📜 Credit Bot')
          .setDescription('Bot ini dibuat dengan ❤️ oleh **Arull**')
          .setColor(0x5865F2),
      ],
    });
    return;
  }

  if (cmd === 'listdb') {
    if (!isOwner) return void msg.reply('🚫 Hanya owner.');
    const names = loadDbList();
    await msg.reply('🗃️ DB terdaftar:\n' + (names.length ? names.map(n => `• ${n}`).join('\n') : '-'));
    return;
  }

  if (cmd === 'setdb') {
    if (!isOwner) return void msg.reply('🚫 Hanya owner.');
    if (args.length === 0) return void msg.reply('⚠️ Usage: !setdb db1,db2');
    const arr = args.join(' ').split(',').map(s => s.trim()).filter(Boolean);
    await saveDbList(arr);
    await msg.reply(`✅ DB list diupdate: ${arr.map(x => `\`${x}\``).join(', ')}`);
    return;
  }

  if (cmd === 'backupnow') {
    if (!isOwner) return void msg.reply('🚫 Hanya owner yang boleh make command ini.');
    await msg.reply('🔁 Otw backup manual, hold my boba...');
    const notify = await client.channels.fetch(NOTIFY_CHANNEL_ID).catch(() => null);
    const fileCh = await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null);
    await backupAllDatabases(notify, fileCh);
    return;
  }
});

if (!DISCORD_TOKEN) {
  console.error('❌ DISCORD_TOKEN belum di-set. Cek file .env kamu.');
  process.exit(1);
}

if (!BACKUP_CHANNEL_ID) console.warn('⚠️ BACKUP_CHANNEL_ID kosong: file hasil backup tidak akan terkirim.');
if (!NOTIFY_CHANNEL_ID) console.warn('⚠️ NOTIFY_CHANNEL_ID kosong: embed ringkasan tidak akan terkirim.');

client.login(DISCORD_TOKEN);
