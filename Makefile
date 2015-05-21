SHELL := bash
BIN := node_modules/.bin
DTS := async/async moment/moment node/node form-data/form-data request/request yargs/yargs

all: server.js

type_declarations: $(DTS:%=type_declarations/DefinitelyTyped/%.d.ts)
type_declarations/DefinitelyTyped/%:
	mkdir -p $(@D)
	curl -s https://raw.githubusercontent.com/chbrown/DefinitelyTyped/master/$* > $@

$(BIN)/tsc:
	npm install

%.js: %.ts type_declarations | $(BIN)/tsc
	$(BIN)/tsc -m commonjs -t ES5 $<
