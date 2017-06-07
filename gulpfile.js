const gulp = require('gulp');
const tasks = require('davis-shared').build;
const config = require('./gulp.config')();
const argv = require('yargs').argv;

const drillPath = argv.drill;
const allJs = config.allJs(drillPath);
const testFiles = config.testFiles(drillPath);

gulp.task('test', tasks.test(testFiles, argv.ci));

gulp.task('lint', tasks.lint(allJs, argv.ci));

gulp.task('watch', function() {
  gulp.watch(allJs, ['lint', 'test']);
});

gulp.task('bump', tasks.bump(['./package.json'], argv.level || 'patch'));

gulp.task('default', ['watch', 'lint', 'test']);
