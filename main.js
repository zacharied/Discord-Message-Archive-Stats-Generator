import fs from 'fs';
import jsonStream from 'JSONStream';
import stripComments from 'strip-json-comments';
import streamPromise from 'stream/promises';

const CHATS_DIR = 'chats';

const files = fs.readdirSync(CHATS_DIR);

const extraUserData = JSON.parse(stripComments(fs.readFileSync('userData.jsonc').toString()));
const userStats = {};

for (let file of files) {
    const jsonParser = jsonStream.parse('messages.*.author');
    let currentHomeChannelOwner = null;

    jsonParser.on('data', function(data) {
        if (data['discriminator'] === '0000' || data['isBot'] === true)
            return;

        if (!(data['id'] in userStats)) {
            userStats[data['id']] = {name: data['name'] + '#' + data['discriminator'], count: 0, homeChannelActivity: 0, ownHomeChannelMessages: 0};
        }

        userStats[data['id']].count++;
        if (currentHomeChannelOwner != null) {
            userStats[currentHomeChannelOwner].homeChannelActivity++;
            if (data['id'] === currentHomeChannelOwner)
                userStats[currentHomeChannelOwner].ownHomeChannelMessages++;
        }
    }, {});

    jsonParser.on('header', function(data) {
        for (const [key, value] of Object.entries(extraUserData)) {
            if (value['homeChannel'] == data['channel']['id']) {
                currentHomeChannelOwner = key;
                break;
            }
        }
    });

    const fileStream = fs.createReadStream(CHATS_DIR + '/' + file);
    await streamPromise.pipeline(fileStream, jsonParser);
}

const outputFile = fs.createWriteStream('output.csv');

outputFile.write('Name,Total Messages,All Messages in Home Channel,Own Messages in Home Channel\n')
for (let val of Object.values(userStats)) {
    outputFile.write(`${val.name},${val.count},${val.homeChannelActivity},${val.ownHomeChannelMessages}\n`)
}
outputFile.close();