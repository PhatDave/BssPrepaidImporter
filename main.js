const fs = require("fs");
const {Client} = require('pg')
const cluster = require("node:cluster")
const process = require("node:process");
const cliProgress = require('cli-progress');

const TEMP_TABLE_DROP = "DROP TABLE IF EXISTS subscriber_billings_temp;";
const TEMP_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS subscriber_billings_temp AS TABLE subscriber_billings WITH NO DATA;";
const INSERT_QUERY = "INSERT INTO subscriber_billings_temp(msisdn, prepaid) VALUES";
const MERGE_QUERY = "INSERT INTO subscriber_billings(msisdn, prepaid) SELECT msisdn, prepaid from subscriber_billings_temp ON CONFLICT (msisdn) DO NOTHING"

const CONNECTION_RE = /([a-zA-Z0-9]+):([a-zA-Z0-9]+)@([a-zA-Z0-9-.:,]+):([0-9]+)\/([a-zA-Z0-9]+)/

const FgRed = "\x1b[31m"
const FgGreen = "\x1b[32m"
const FgYellow = "\x1b[33m"
const FgWhite = "\x1b[37m"

function log(message) {
	console.log(`${FgWhite}${message}${FgWhite}`);
}

function logError(message) {
	log(`${FgRed}${message}${FgRed}`);
}

function getArg(index, check, defaultValue, errorMessage) {
	if (process.argv[index] == null) {
		if (defaultValue == null) {
			logError(`Parameter ${index} is missing`);
			process.exit(1);
		}
		return defaultValue;
	}
	let value = process.argv[index]
	if (check(value)) {
		return value;
	} else {
		logError(errorMessage);
		process.exit(1);
	}
}

if (process.argv.length < 7) {
	log(FgRed + "Insufficient parameters")
	log(`Usage: ${FgYellow}main-<system> <textFile1> <textFile2> <connectionString> <batchSize>`);
	log("Text files are csv files containing msisdn and prepaid columns");
	log(`Connection string is expected in the form of ${FgYellow}user:password@host:port/database`);
	log(`Batch size is the number of records to process in each batch, does affect performance greatly and should be kept reasonable, recommended < 16k`);
	log(`Example: ${FgGreen}./main-win.exe prepaid_true.txt prepaid_false.txt bss:bss@localhost:5434/bss 8192`);
	log(`Example: ${FgGreen}./main-linux prepaid_true.txt prepaid_false.txt bss:bss@localhost:5434/bss 8192`);
	process.exit(1);
} else {
	let fileTrue = getArg(2, (x) => fs.existsSync(x), null, "File does not exist");
	let fileFalse = getArg(3, (x) => fs.existsSync(x), null, "File does not exist");
	let connectionString = getArg(4, (x) => {
		return CONNECTION_RE.exec(x) !== null && CONNECTION_RE.exec(x).length === 6;
	}, null, "Invalid connection string");
	let batchSize = getArg(5, (x) => x >= 0, 1, "Invalid batch size");

	// Setup db
	const connectionMatch = CONNECTION_RE.exec(connectionString);
	const username = connectionMatch[1]
	const password = connectionMatch[2]
	const host = connectionMatch[3]
	const port = connectionMatch[4]
	const database = connectionMatch[5]

	async function setupTempTable() {
		return new Promise(async (resolve, reject) => {
			let client = new Client({
				user: username,
				host: host,
				database: database,
				password: password,
				port: port,
			});

			log(`Connecting to database ${FgGreen}${database}`);
			await client.connect();
			log(`Connected to ${FgGreen}${database}`);
			await client.query(TEMP_TABLE_DROP);
			log('Dropped temporary table');
			await client.query(TEMP_TABLE_CREATE);
			log('Created temporary table');
			resolve();
		});
	}

	async function mergeTables() {
		return new Promise(async (resolve, reject) => {
			let client = new Client({
				user: username,
				host: host,
				database: database,
				password: password,
				port: port,
			});

			log(`Connecting to database ${FgGreen}${database}`);
			await client.connect();
			log(`Connected to ${FgGreen}${database}`);
			log(`Merging tables (could take a minute) ...`);
			await client.query(MERGE_QUERY);
			log('Dropping temporary table ...');
			await client.query(TEMP_TABLE_DROP);
			log(`${FgGreen}Done`);
			resolve();
		});
	}

	setupTempTable().then(() => {
		// Setup data
		let prepaidTrueData = fs.readFileSync(fileTrue, "utf8").trim().split("\n");
		let prepaidFalseData = fs.readFileSync(fileFalse, "utf8").trim().split("\n");

		// Remove header
		prepaidTrueData = prepaidTrueData.splice(1);
		prepaidFalseData = prepaidFalseData.splice(1);

		let fileData = [];
		prepaidTrueData.forEach((item) => {
			let data = item.split(",");
			data = {
				msisdn: data[0].trim(),
				prepaid: data[1].trim(),
			}
			fileData.push(data);
		});
		prepaidFalseData.forEach((item) => {
			let data = item.split(",");
			data = {
				msisdn: data[0].trim(),
				prepaid: data[1].trim(),
			}
			fileData.push(data);
		});
		log(`Loaded ${fileData.length} msisdns`);

		class QueryManager {
			itemsToInsert = [];

			constructor(client, batchSize) {
				this.client = client;
				this.batchSize = batchSize;
			}

			async insert(item) {
				return new Promise(async (resolve, reject) => {
					if (this.itemsToInsert.length >= this.batchSize) {
						await this.doInsertQuery();
					}
					this.itemsToInsert.push(item);

					resolve();
				});
			}

			doInsertQuery() {
				return new Promise(async (resolve, reject) => {
					let query = INSERT_QUERY;
					this.itemsToInsert.forEach((item) => {
						query += `('${item.msisdn}', ${item.prepaid}),`;
					});
					query = query.slice(0, -1);
					query += ";";

					// log(`Inserting ${this.itemsToInsert.length} rows`);

					await this.client.query(query).then(() => {
						// log(`Inserted ${this.itemsToInsert.length} rows`);
						this.itemsToInsert = [];
						resolve();
					});

					// this.itemsToInsert = [];
					// resolve();
				});
			}
		}

		// To shorten data for debug
		// fileData = fileData.splice(0, 1000);
		// Run
		let client = new Client({
			user: username,
			host: host,
			database: database,
			password: password,
			port: port,
		});
		let pbar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
		pbar.start(fileData.length, 0);
		client.connect().then(async () => {
			let queryManager = new QueryManager(client, batchSize);
			for (let i = 0; i < fileData.length; i++) {
				let item = fileData[i];
				await queryManager.insert(item);
				pbar.increment();
				if (i === fileData.length - 2) {
					await queryManager.doInsertQuery();
				}
			}
			log(`${FgGreen}INSERT finished`);
			mergeTables().then(() => process.exit(0));
		});
	});
}
