'use strict';

const Hapi = require('hapi');
global.Promise = require('bluebird');
global.mongoose = require('mongoose');
mongoose.Promise = Promise;

const server = new Hapi.Server();

server.connection({
    host: 'localhost',
    port: 3000
});

let Schema = mongoose.Schema;

let OrderSchema = new Schema({
    orderId: { type: String, required: true, unique: true },
    companyName: { type: String, default: null },
    customerAddress: { type: String, default: null },
    orderedItem: { type: String, default: null }
}, {
    timestamps: true
});

let Order = mongoose.model('order', OrderSchema);

let dbUrl = 'mongodb://localhost:27017/orders';

let dbOtions = {
    useMongoClient: true
};

server.route({
    method: 'POST',
    path: '/orders',
    handler: function(request, reply) {
        insertOrders(request.payload, reply);
    }
});

server.route({
    method: 'POST',
    path: '/bulkInsertData',
    handler: function(request, reply) {
        let inputData = "001, SuperTrader, Steindamm 80, Macbook\n002, Cheapskates, Reeperbahn 153, Macbook\n003, MegaCorp, Steindamm 80, Book \"Guide to Hamburg\"\n004, SuperTrader, Sternstrasse  125, Book \"Cooking  101\"\n005, SuperTrader, Ottenser Hauptstrasse 24, Inline Skates\n006, MegaCorp, Reeperbahn 153, Playstation\n007, Cheapskates, Lagerstrasse  11, Flux compensator\n008, SuperTrader, Reeperbahn 153, Inline Skates"
        let inputArray = inputData.split("\n");
        let arr = [];

        for (let i = 0; i < inputArray.length; i += 1) {
            let row = inputArray[i].split(",");
            arr.push({
                'orderId': row[0].trim(),
                'companyName': row[1].trim(),
                'customerAddress': row[2].trim(),
                'orderedItem': row[3].trim()
            });
        }

        if (arr.length > 0) {
            bulkInsertData(arr, function(err, data) {
                if (err) {
                    reply({
                        statusCode: 400,
                        message: "Error in bulk insert",
                        data: err
                    });
                } else {
                    reply({
                        statusCode: 200,
                        message: "Inserted successfully!",
                        data: data
                    });
                }
            })
        } else {
            reply({
                statusCode: 400,
                message: "Please provide input to insert",
                data: {}
            });
        }
    }
});

server.route({
    method: 'GET',
    path: '/ordersByCompanyName',
    handler: function(request, reply) {
        getParticularOrdersOfCompany(request.query.companyName, reply);
    }
});

server.route({
    method: 'GET',
    path: '/ordersByCustomerAddress',
    handler: function(request, reply) {
        getParticularOrdersOfAddress(request.query.customerAddress, reply);
    }
});

server.route({
    method: 'GET',
    path: '/displayOrdersPerOrderedItem',
    handler: function(request, reply) {
        displayOrdersPerOrderedItem(request, reply);
    }
});

server.route({
    method: 'DELETE',
    path: '/orders',
    handler: function(request, reply) {
        removeOrder(request.query.orderId, reply);
    }
});

server.start((err) => {

    if (err) {
        throw err;
    } else {
        mongoose.connect(dbUrl, dbOtions, function(err) {
            if (err) server.log('error', err);
        });
        console.log(`Server running at: ${server.info.uri}`);
    }
});


let insertOrders = function(payload, reply) {

    try {
        let newOrder = new Order(payload);
        newOrder.save(function(err) {
            if (err) {
                if (err.code === 11000) {
                    reply({
                        statusCode: 209,
                        message: "Duplicate Entry",
                        data: payload
                    });
                } else {
                    reply(err.message);
                }
            } else {
                console.log("record saved successfully");
                reply({
                    statusCode: 200,
                    message: "record saved successfully"
                });
            }
        });

    } catch (err) {
        reply(err);
    }
}


let getParticularOrdersOfCompany = function(companyName, reply) {

    try {

        Order.find({
            companyName: companyName
        }, {
            _id: 1,
            orderId: 1,
            companyName: 1,
            customerAddress: 1,
            orderedItem: 1,
        }, { lean: true }, function(err, data) {
            if (err) {
                console.log(err);
                reply(err);
            } else {
                console.log("record fetched successfully");
                reply({
                    statusCode: 200,
                    message: "record fetched successfully",
                    data: data
                });
            }
        });

    } catch (err) {
        reply(err)
    }
}

let getParticularOrdersOfAddress = function(customerAddress, reply) {

    try {

        Order.find({
            customerAddress: customerAddress
        }, {
            _id: 1,
            orderId: 1,
            companyName: 1,
            customerAddress: 1,
            orderedItem: 1,
        }, { lean: true }, function(err, data) {
            if (err) {
                console.log(err);
                reply(err);
            } else {
                console.log("record fetched successfully");
                reply({
                    statusCode: 200,
                    message: "record fetched successfully",
                    data: data
                });
            }
        });
    } catch (err) {
        reply(err)
    }
}

let removeOrder = function(orderId, reply) {

    try {

        Order.remove({
            orderId: orderId
        }, function(err) {
            if (err) {
                console.log(err);
                reply(err);
            } else {
                console.log("record removed successfully");
                reply({
                    statusCode: 200,
                    message: "record removed successfully"
                });
            }
        });

    } catch (err) {
        reply(err)
    }
}

let displayOrdersPerOrderedItem = function(request, reply) {

    try {
        Order.aggregate([{
            $group: {
                _id: "$orderedItem",
                count: { $sum: 1 }
            }
        }], function(err, data) {
            if (err) {
                console.log(err);
                reply(err);
            } else {
                console.log("record fetched successfully");
                reply({
                    statusCode: 200,
                    message: "record fetched successfully",
                    data: data
                });
            }
        });
    } catch (err) {
        reply(err)
    }
}

let bulkInsertData = function(rawDocuments, callback) {

    try {
        Order.insertMany(rawDocuments, function(error, docs) {
            if (error) {
                console.log("err in bulk insertion ", error);
                callback(error);
            } else {
                console.log("inserted in bulk ");
                callback(null, docs)
            }
        });
    } catch (err) {
        console.log("err in bulk insert : ", err);
        callback(err)
    }
}