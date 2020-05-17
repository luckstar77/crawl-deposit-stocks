const rp = require('request-promise');
const cheerio = require('cheerio');
const _ = require('lodash');
const math = require('mathjs');

const AWS = require('aws-sdk');

AWS.config.update({
  region: 'ap-northeast-1',
});

var docClient = new AWS.DynamoDB.DocumentClient();

AWS.config.update({ region: 'us-west-2' });
const ses = new AWS.SES({ apiVersion: '2010-12-01' });

const DIVIDEND_YIELD = parseFloat(process.env.DIVIDEND_YIELD) || 5;
const EPS_STD_DEV = parseFloat(process.env.EPS_STD_DEV) || 1;

// Create sendEmail params
var params = {
  Destination: {
    /* required */
    CcAddresses: [
      /* more items */
    ],
    ToAddresses: [
      'luckstar77y@gmail.com',
      '497j1005@stust.edu.tw',
      /* more items */
    ],
  },
  Message: {
    /* required */
    Body: {
      /* required */
      // Html: {
      //  Charset: "UTF-8",
      //  Data: "HTML_FORMAT_BODY"
      // },
      Text: {
        Charset: 'UTF-8',
        Data: 'TEXT_FORMAT_BODY',
      },
    },
    Subject: {
      Charset: 'UTF-8',
      Data: '3條均線糾結且外資投信連續買超3日',
    },
  },
  Source: 'luckstar77y@gmail.com' /* required */,
  ReplyToAddresses: [
    /* more items */
  ],
};

const worker = async (SubjectData, isFutures) => {
  let $ = cheerio.load(
    await rp({
      uri: 'https://stock.wespai.com/p/57193',
      headers: {
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
      },
      json: true,
    }),
  );

  let parseDepositStocks = $('#example tbody tr')
    .map((index, stock) => {
      return {
        symbol: $(stock)
          .children('td')
          .eq(0)
          .text(),
        company: $(stock)
          .children('td')
          .eq(1)
          .text(),
        price: parseFloat(
          $(stock)
            .children('td')
            .eq(2)
            .text(),
        ),
        dividendYield: parseFloat(
          $(stock)
            .children('td')
            .eq(3)
            .text(),
        ),
        s4eps: parseFloat(
          $(stock)
            .children('td')
            .eq(4)
            .text(),
        ),
        yeps: parseFloat(
          $(stock)
            .children('td')
            .eq(5)
            .text(),
        ),
        y1eps: parseFloat(
          $(stock)
            .children('td')
            .eq(6)
            .text(),
        ),
        y2eps: parseFloat(
          $(stock)
            .children('td')
            .eq(7)
            .text(),
        ),
        mRevenueYoY: parseFloat(
          $(stock)
            .children('td')
            .eq(8)
            .text(),
        ),
        m1RevenueYoY: parseFloat(
          $(stock)
            .children('td')
            .eq(9)
            .text(),
        ),
        m2RevenueYoY: parseFloat(
          $(stock)
            .children('td')
            .eq(10)
            .text(),
        ),
        mCumulativeRevenueYoY: parseFloat(
          $(stock)
            .children('td')
            .eq(11)
            .text(),
        ),
        s4opm: parseFloat(
          $(stock)
            .children('td')
            .eq(12)
            .text(),
        ),
        yopm: parseFloat(
          $(stock)
            .children('td')
            .eq(13)
            .text(),
        ),
        y1opm: parseFloat(
          $(stock)
            .children('td')
            .eq(14)
            .text(),
        ),
        y2opm: parseFloat(
          $(stock)
            .children('td')
            .eq(15)
            .text(),
        ),
        s4nim: parseFloat(
          $(stock)
            .children('td')
            .eq(16)
            .text(),
        ),
        ynim: parseFloat(
          $(stock)
            .children('td')
            .eq(17)
            .text(),
        ),
        y1nim: parseFloat(
          $(stock)
            .children('td')
            .eq(18)
            .text(),
        ),
        y2nim: parseFloat(
          $(stock)
            .children('td')
            .eq(19)
            .text(),
        ),
        cashM: parseFloat(
          $(stock)
            .children('td')
            .eq(20)
            .text(),
        ),
        qr: parseFloat(
          $(stock)
            .children('td')
            .eq(21)
            .text(),
        ),
      };
    })
    .get();

  parseDepositStocks = parseDepositStocks.reduce((accu, stock) => {
    if (stock.dividendYield < DIVIDEND_YIELD) return accu;
    let dividend = stock.price * (stock.dividendYield / 100);
    if (stock.s4eps <= dividend) return accu;
    let eps = [stock.s4eps, stock.yeps, stock.y1eps, stock.y2eps];
    let epsAvg = math.mean(eps);
    if (epsAvg <= 0) return accu;
    let epsStdDev = math.std(eps);
    if (epsStdDev > EPS_STD_DEV) return accu;

    let revenue = [
      stock.mRevenueYoY,
      stock.m1RevenueYoY,
      stock.m2RevenueYoY,
      stock.mCumulativeRevenueYoY,
    ];
    if (_.filter(revenue, r => r <= 0).length > 1) return accu;

    let opm = [stock.s4opm, stock.yopm, stock.y1opm, stock.y2opm];
    if (_.filter(opm, r => r < 0).length > 0) return accu;

    let nim = [stock.s4nim, stock.ynim, stock.y1nim, stock.y2nim];
    if (_.filter(nim, r => r < 0).length > 0) return accu;

    accu.push({
      ...stock,
      epsAvg,
      epsStdDev,
      dividend,
    });
    return accu;
  }, []);

  if (isFutures) {
    $ = cheerio.load(await rp('https://www.taifex.com.tw/cht/2/stockLists'));
    const stockFutures = _.map($('#myTable tbody tr'), item => ({
      stockFutureSymbol: $(item)
        .children('td')
        .eq(0)
        .text(),
      twTitleFull: $(item)
        .children('td')
        .eq(1)
        .text(),
      symbol: $(item)
        .children('td')
        .eq(2)
        .text(),
      twTitle: $(item)
        .children('td')
        .eq(3)
        .text(),
      isStockFutureUnderlying: $(item)
        .children('td')
        .eq(4)
        .text()
        .trim()
        ? true
        : false,
      isStockOptionUnderlying: $(item)
        .children('td')
        .eq(5)
        .text()
        .trim()
        ? true
        : false,
      isStockExchangeUnderlying: $(item)
        .children('td')
        .eq(6)
        .text()
        .trim()
        ? true
        : false,
      isOTCUnderlying: $(item)
        .children('td')
        .eq(7)
        .text()
        .trim()
        ? true
        : false,
      isStockExchangeETFUnderlying: $(item)
        .children('td')
        .eq(8)
        .text()
        .trim()
        ? true
        : false,
      NumberOfStock: parseInt(
        $(item)
          .children('td')
          .eq(9)
          .text()
          .replace(',', ''),
      ),
    }));

    parseDepositStocks = parseDepositStocks.reduce((accu, curr) => {
      for (let stockFuture of stockFutures) {
        if (curr.symbol !== stockFuture.symbol) continue;

        accu.push({ ...curr, ...stockFuture });
        break;
      }
      return accu;
    }, []);
  }

  if (_.isEmpty(parseDepositStocks)) {
    params.Message.Body.Text.Data = JSON.stringify(
      {
        parseDepositStocks,
      },
      null,
      2,
    );
    params.Message.Subject.Data = SubjectData + '_沒有匹配';
    return await ses.sendEmail(params).promise();
  }

  for (let stock of parseDepositStocks) {
    await new Promise(resolve => setTimeout(() => resolve(), 500));
    $ = cheerio.load(
      await rp({
        uri: 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp',
        qs: {
          STOCK_ID: stock.symbol,
        },
        headers: {
          'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        },
        json: true,
      }),
    );
    stock.dividendCount = 0;
    stock.cashCount = 0;
    stock.rightCount = 0;
    stock.cashRecoveredCount = 0;
    stock.rightRecoveredCount = 0;
    stock.cashRecoveredRate = 0;
    stock.rightRecoveredRate = 0;
    stock.avgDividendYield = 0;
    stock.sumDividendYield = 0;

    const dividendList = _.reduce(
      $('#divDetail tbody tr'),
      (accu, item) => {
        const year = $(item)
          .children('td')
          .eq(0)
          .text();
        const dividend =
          $(item)
            .children('td')
            .eq(7)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(7)
                  .text(),
              )
            : 0;
        const cashRecoveredDay =
          $(item)
            .children('td')
            .eq(10)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(10)
                  .text(),
              )
            : 0;
        const rightRecoveredDay =
          $(item)
            .children('td')
            .eq(11)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(11)
                  .text(),
              )
            : 0;
        const cashTotal =
          $(item)
            .children('td')
            .eq(3)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(3)
                  .text(),
              )
            : 0;
        const rightTotal =
          $(item)
            .children('td')
            .eq(6)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(6)
                  .text(),
              )
            : 0;
        const dividendYield =
          $(item)
            .children('td')
            .eq(18)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(18)
                  .text(),
              )
            : 0;
        if (year === '累計') return accu;
        if (!(dividend > 0)) return accu;

        if (!(cashTotal > 0) && !(rightTotal > 0)) return accu;
        stock.dividendCount++;
        stock.sumDividendYield += dividendYield;

        if (cashTotal > 0) stock.cashCount++;
        if (rightTotal > 0) stock.rightCount++;
        if (cashRecoveredDay && cashRecoveredDay <= 366)
          stock.cashRecoveredCount++;
        if (rightRecoveredDay && rightRecoveredDay <= 366)
          stock.rightRecoveredCount++;

        accu.push({
          year,
          cashSurplus:
            $(item)
              .children('td')
              .eq(1)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(1)
                    .text(),
                )
              : 0,
          cashAdditionalPaidIn:
            $(item)
              .children('td')
              .eq(2)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(2)
                    .text(),
                )
              : 0,
          cashTotal,
          rightSurplus:
            $(item)
              .children('td')
              .eq(4)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(4)
                    .text(),
                )
              : 0,
          rightAdditionalPaidIn:
            $(item)
              .children('td')
              .eq(5)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(5)
                    .text(),
                )
              : 0,
          rightTotal,
          dividend,
          cashB:
            $(item)
              .children('td')
              .eq(8)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(8)
                    .text(),
                )
              : 0,
          rightK:
            $(item)
              .children('td')
              .eq(9)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(9)
                    .text(),
                )
              : 0,
          cashRecoveredDay,
          rightRecoveredDay,
          year1:
            $(item)
              .children('td')
              .eq(12)
              .text() !== '-',
          maxPrice:
            $(item)
              .children('td')
              .eq(13)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(13)
                    .text(),
                )
              : 0,
          minPrice:
            $(item)
              .children('td')
              .eq(14)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(14)
                    .text(),
                )
              : 0,
          avgPrice:
            $(item)
              .children('td')
              .eq(15)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(15)
                    .text(),
                )
              : 0,
          cashDividendYield:
            $(item)
              .children('td')
              .eq(16)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(16)
                    .text(),
                )
              : 0,
          rightDividendYield:
            $(item)
              .children('td')
              .eq(17)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(17)
                    .text(),
                )
              : 0,
          dividendYield,
          year2: $(item)
            .children('td')
            .eq(19)
            .text(),
          eps:
            $(item)
              .children('td')
              .eq(20)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(20)
                    .text(),
                )
              : 0,
          cashDPR:
            $(item)
              .children('td')
              .eq(21)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(21)
                    .text(),
                )
              : 0,
          rightDPR:
            $(item)
              .children('td')
              .eq(22)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(22)
                    .text(),
                )
              : 0,
          dpr:
            $(item)
              .children('td')
              .eq(23)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(23)
                    .text(),
                )
              : 0,
        });
        return accu;
      },
      [],
    );

    if (stock.cashCount > 0)
      stock.cashRecoveredRate = stock.cashRecoveredCount / stock.cashCount;
    if (stock.rightCount > 0)
      stock.rightRecoveredRate = stock.rightRecoveredCount / stock.rightCount;
    if (stock.dividendCount > 0)
      stock.avgDividendYield = stock.sumDividendYield / stock.dividendCount;
  }

  parseDepositStocks = _.orderBy(parseDepositStocks, 'dividendYield', 'desc');

  console.log(JSON.stringify(parseDepositStocks, null, 2));

  for (let i = 0; i <= parseInt(parseDepositStocks.length / 25); i++) {
    const list = parseDepositStocks.slice(i * 25, (i + 1) * 25);
    if (_.isEmpty(list)) break;

    await new Promise((resolve, reject) => {
      const now = new Date();
      docClient.batchWrite(
        {
          RequestItems: {
            dividends: list.map(o => ({
              PutRequest: {
                Item: {
                  ...o,
                  created: now.getTime(),
                },
              },
            })),
          },
        },
        function(err, data) {
          if (err) {
            console.error(
              'Unable to add item. Error JSON:',
              JSON.stringify(err, null, 2),
            );
            console.log(list);
            reject(err);
          } else {
            console.log('Added item:', JSON.stringify(data, null, 2));
            resolve(data);
          }
        },
      );
    });
  }

  params.Message.Body.Text.Data = JSON.stringify(parseDepositStocks, null, 2);
  params.Message.Subject.Data = SubjectData;
  return await ses.sendEmail(params).promise();
};

exports.handler = async function(event, context) {
  await worker('殖利率大於5%且EPS營收穩定_期貨', true);
  await worker('殖利率大於5%且EPS營收穩定_股票', false);
  return 'ok';
};
