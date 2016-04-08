# This program was created by Rafael Rocha de Carvalho for the class of Distributed Data Management taught at UFPR by Professor Eduardo Almeida. 
# This code is intended to implement the DHT using a test.in input file and writing the output to file test.out

class Operations
    
    attr_accessor :operationId, :operation, :operationElement, :operationElement2 
    
    def initialize(op, opElement, opElement2)  
    # Instance variables  
        @operation = op 
        @operationElement = opElement
        @operationElement2 = opElement2
    end
end

$endNode = 0
$data = Array.new # array that receive the data from input file
$opHash =  Hash.new


#TODO: Read from stdin
def readFile
    File.open(ARGV[0], "r") do |f|
      f.each_line do |line|
        if line != " " or line != ""
            auxData = line.split
            n = Operations.new(auxData[1],auxData[2],auxData[3])
            $data.push(n)
        end
      end
    end
end

# create the keys that an node will guard
def createKeys(keyvalue)
    keys = Array.new
    max = keyvalue - 1
    max.downto(0) { 
        |i| 
        if($opHash[i] == nil || $opHash[i] == "")
            keys.push(i)
        else
            break
        end
    }
    return keys
end

def updateTable (keyValue)
    for i in (keyValue + 1)  .. $endNode
        if($opHash[i] != nil && $opHash[i] != "")
            $opHash[i] = createKeys(i)
            break
        end
    end
end

def createHash(operation)
    $opHash[Integer(operation.operationElement)] = createKeys(Integer(operation.operationElement))
    if(Integer(operation.operationElement) > $endNode)
        $endNode = Integer(operation.operationElement)
    end
    updateTable(Integer(operation.operationElement))
end


# Method that test the data received from input file
def testDataReceived
    #p $data
    $data.each { 
        |operation|
        puts operation.operation
        puts operation.operationElement
        if(operation.operation == "E")
            createHash(operation)
        end    
    }
end

readFile
testDataReceived

puts $opHash