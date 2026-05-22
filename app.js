'use strict';

const JackeryApp = require('./lib/JackeryApp');

module.exports = class MyApp extends JackeryApp {

  async onInit() {
    await super.onInit();
    this.log('Jackery App has been initialized');
  }

};
