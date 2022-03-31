import fs from 'fs';
import jsonStream from 'JSONStream';
import stripComments from 'strip-json-comments';
import streamPromise from 'stream/promises';

const CHATS_DIR = 'chats';

const files = fs.readdirSync(CHATS_DIR);

const extraUserData = JSON.parse(stripComments(fs.readFileSync('userData.jsonc').toString()));
const channelRelationTypes = [];
const userStats = {};

for (const [userId, userData] of Object.entries(extraUserData)) {
    userStats[userId] = {
        // We will find name when iterating thru messages.
        name: null,
        totalMessageCount: 0,
        relationStats: []
    };

    if ('channels' in userData)
        for (const channel of userData['channels'])
            if (channelRelationTypes.indexOf(channel['relation']) === -1)
                channelRelationTypes.push(channel['relation']);
}

for (let file of files) {
    const jsonParser = jsonStream.parse('messages.*.author');
    let channelRelations = [];

    jsonParser.on('data', function(data) {
        const userId = data['id'];

        if (data['isBot'] === true)
            return;

        if (!(userId in userStats)) {
            console.warn(`Unknown user ${data['name']}`)
            return;
        }

        if (userStats[userId].name == null)
            userStats[userId].name = data['name'] + '#' + data['discriminator'];

        userStats[userId].totalMessageCount++;

        for (let channelRelation of channelRelations) {
            if (!userStats[channelRelation.user].relationStats.some(rel => rel.relation === channelRelation.relation))
                userStats[channelRelation.user].relationStats.push({ relation: channelRelation.relation, ownMessageCount: 0, totalMessageCount: 0 });
        
            const thisRelationStats = userStats[channelRelation.user].relationStats.find(rel => rel.relation === channelRelation.relation);
            thisRelationStats.totalMessageCount++;
            if (userId === channelRelation.user)
                thisRelationStats.ownMessageCount++;
        }
    }, {});

    jsonParser.on('header', function(data) {
        for (const [userId, extraData] of Object.entries(extraUserData)) {
            for (const channel of extraData['channels'])
                if (channel['id'] === data['channel']['id'])
                    channelRelations.push({ user: userId, relation: channel['relation']})
        }
    });

    const fileStream = fs.createReadStream(CHATS_DIR + '/' + file);
    await streamPromise.pipeline(fileStream, jsonParser);
}

const outputFile = fs.createWriteStream('output.csv');

outputFile.write('Name,Total Messages');
for (const relationName of channelRelationTypes)
    outputFile.write(`,All Messages in ${relationName} Channel(s),Own Messages in ${relationName} Channel(s)`);
outputFile.write('\n');

for (let val of Object.values(userStats)) {
    outputFile.write(`${val.name},${val.totalMessageCount}`);
    for (const relationName of channelRelationTypes) {
        if (val.relationStats.some(rel => rel.relation === relationName)) {
            const thisRelationStats = val.relationStats.find(rel => rel.relation === relationName);
            outputFile.write(`,${thisRelationStats.totalMessageCount},${thisRelationStats.ownMessageCount}`);
        } else {
            outputFile.write(',,');
        }
    }
    outputFile.write('\n');
}
outputFile.close();